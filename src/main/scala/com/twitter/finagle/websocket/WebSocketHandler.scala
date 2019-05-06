package com.twitter.finagle.websocket

import java.net.URI

import com.twitter.concurrent.{Broker, Offer}
import com.twitter.finagle.CancelledRequestException
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util._
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel._
import io.netty.handler.codec.http._
import io.netty.handler.codec.http.websocketx._
import io.netty.util.CharsetUtil

class WebSocketHandler extends ChannelDuplexHandler {
  protected[this] val messagesBroker = new Broker[String]
  protected[this] val binaryMessagesBroker = new Broker[Array[Byte]]
  protected[this] val pingsBroker = new Broker[Array[Byte]]
  protected[this] val closer = new Promise[Unit]
  protected[this] val timer = DefaultTimer.twitter

  protected def toByteArray(buffer: ByteBuf): Array[Byte] = {
    val array = new Array[Byte](buffer.readableBytes())
    buffer.readBytes(array)
    buffer.release()
    array
  }

  private[this] class ListenerImpl(promise:Promise[Unit]) extends ChannelFutureListener {
    def operationComplete(cf: ChannelFuture) {
      if(cf.isSuccess)
        promise.setValue(())
      else if(cf.isCancelled)
        promise.setException(new CancelledRequestException)
      else
        promise.setException(cf.cause)
    }
  }

  protected[this] def channelFutureToOffer(future: ChannelFuture): Offer[Try[Unit]] = {
    val promise = new Promise[Unit]
    future.addListener(new ListenerImpl(promise))
    promise.toOffer
  }


  protected[this] def writeWebSocket(ctx: ChannelHandlerContext, sock: WebSocket, promise: ChannelPromise, ack: Option[Offer[Try[Unit]]] = None): Unit = {
    def close() {
      sock.close()
      if (ctx.channel.isOpen) ctx.channel.close()
    }

    val awaitAck = ack match {
      case Some(ackOffer) =>
        ackOffer {
          case Return(_) => writeWebSocket(ctx, sock, promise, None)
          case Throw(_) => close()
        }

      case None =>
        Offer.choose(
          sock.messages {
            message =>
              val frame = new TextWebSocketFrame(message)
              val writeFuture = ctx.writeAndFlush(frame)
              writeWebSocket(ctx, sock, promise, Some(channelFutureToOffer(writeFuture)))

          },
          sock.binaryMessages {
            binary =>
              val frame = new BinaryWebSocketFrame(Unpooled.copiedBuffer(binary))
              val writeFuture = ctx.writeAndFlush(frame)
              writeWebSocket(ctx, sock, promise, Some(channelFutureToOffer(writeFuture)))
          })

    }

    awaitAck.sync()
  }

}

class WebSocketServerHandler extends WebSocketHandler {

  private[this] var handshaker: Option[WebSocketServerHandshaker] = None

  override def channelRead(ctx: ChannelHandlerContext, e: AnyRef): Unit = {
    e match {
      case req: FullHttpRequest =>
        try {
          val scheme = if(req.getUri.startsWith("wss")) "wss" else "ws"
          val location = scheme + "://" + req.headers.get(HttpHeaders.Names.HOST) + "/"
          val wsFactory = new WebSocketServerHandshakerFactory(location, null, false)
          handshaker = Option(wsFactory.newHandshaker(req))
          handshaker match {
            case None =>
              WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel)

            case Some(h) =>
              h.handshake(ctx.channel, req).addListener(new ChannelFutureListener {
                override def operationComplete(future: ChannelFuture): Unit = {
                  if (!future.isSuccess)
                    ctx.fireExceptionCaught(future.cause)
                }
              })

              def close(): ChannelFuture = ctx.channel().close()

              val webSocket = WebSocket(
                messages = messagesBroker.recv,
                binaryMessages = binaryMessagesBroker.recv,
                pings = pingsBroker.recv,
                uri = new URI(req.uri),
                remoteAddress = ctx.channel.remoteAddress,
                onClose = closer,
                close = close)

              ctx.fireChannelRead(webSocket)
          }
        } finally {
          req.release()
        }

      case frame: CloseWebSocketFrame =>
        handshaker match {
          case None =>
            ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE)
          case Some(h) =>
            frame.retain()
            h.close(ctx.channel, frame)
        }

      case frame: PingWebSocketFrame =>
        val frameContent = frame.content()
        frameContent.retain()

        pingsBroker ! toByteArray(frameContent)
        ctx.writeAndFlush(new PongWebSocketFrame(frameContent))

      case frame: TextWebSocketFrame =>
        messagesBroker ! frame.text

      case frame: BinaryWebSocketFrame =>
        binaryMessagesBroker ! toByteArray(frame.content)

      case invalid =>
        ctx.fireExceptionCaught(new IllegalArgumentException("invalid message \"%s\"".format(invalid)))
    }
  }

  override def write(ctx: ChannelHandlerContext, e: AnyRef, promise: ChannelPromise): Unit = {
    e match {
      case sock: WebSocket =>
        writeWebSocket(ctx, sock, promise)

      case _: HttpResponse =>
        ctx.write(e, promise)

      case _: PongWebSocketFrame =>
        ctx.write(e, promise)

      case _: CloseWebSocketFrame =>
        ctx.write(e, promise)

      case invalid =>
        ctx.fireExceptionCaught(new IllegalArgumentException("invalid message \"%s\"".format(invalid)))
    }
  }
}

class WebSocketClientHandler extends WebSocketHandler {

  @volatile private[this] var handshaker: Option[WebSocketClientHandshaker] = None
  private[this] var keepAliveTask: Option[TimerTask] = None

  private def replyClientImmediatly(ctx: ChannelHandlerContext, e: AnyRef, promise: ChannelPromise, sock: WebSocket) = {
    def close() {
      keepAliveTask.foreach(_.close())
      ctx.channel().close()
    }

    val webSocket = sock.copy(
      messages = messagesBroker.recv,
      binaryMessages = binaryMessagesBroker.recv,
      onClose = closer,
      close = close)

    ctx.fireChannelRead(webSocket)
    promise.setSuccess()
  }

  private def createWebSocketHandshake(ctx: ChannelHandlerContext, sock: WebSocket) = {
    def initiateKeepAlive(): Unit =
      for (interval <- sock.keepAlive) {
        keepAliveTask = Option(timer.schedule(interval) {
          ctx.writeAndFlush(new PingWebSocketFrame())
        })
      }

    val hs = WebSocketClientHandshakerFactory.newHandshaker(sock.uri, sock.version, null, false, new DefaultHttpHeaders())
    handshaker = Some(hs)

    hs.handshake(ctx.channel()).addListener(new ChannelFutureListener {
      override def operationComplete(future: ChannelFuture): Unit = initiateKeepAlive
    })
  }

  override def channelRead(ctx:ChannelHandlerContext, msg: AnyRef):Unit = {
    if(! handshaker.get.isHandshakeComplete)
      handshaker.get.finishHandshake(ctx.channel, msg.asInstanceOf[FullHttpResponse])
    else
      msg match {
        case res:FullHttpResponse =>
          throw new IllegalStateException(s"ERROR: Unexpected FullHttpResponse (status=${res.status.code}, content=${res.content().toString(CharsetUtil.UTF_8)})")

        case frame:CloseWebSocketFrame =>
          ctx.channel.close()

        case frame:PongWebSocketFrame =>
        //          pongReceived = true

        case frame:TextWebSocketFrame =>
          messagesBroker ! frame.text

        case frame: BinaryWebSocketFrame =>
          binaryMessagesBroker ! toByteArray(frame.content)

        case invalid =>
          ctx.fireExceptionCaught(new IllegalArgumentException("invalid message \"%s\"".format(invalid)))

      }
  }

  override def write(ctx: ChannelHandlerContext, e: AnyRef, promise: ChannelPromise): Unit = {
    e match {
      case sock: WebSocket =>
        writeWebSocket(ctx, sock, promise)
        replyClientImmediatly(ctx, e, promise, sock)
        createWebSocketHandshake(ctx, sock)

      case _: HttpRequest =>
        ctx.write(e, promise)

      case _: CloseWebSocketFrame =>
        ctx.write(e, promise)

      case _: PingWebSocketFrame =>
        ctx.write(e, promise)

      case invalid =>
        ctx.fireExceptionCaught(new IllegalArgumentException("invalid message \"%s\"".format(invalid)))
    }
  }
}
