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
import io.netty.handler.timeout.IdleStateEvent
import scala.collection.JavaConverters._

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

  protected[this] def listenWebSocket(ctx: ChannelHandlerContext, sock: WebSocket,
                                      promise: ChannelPromise, ack: Option[Offer[Try[Unit]]] = None): Unit = {
    def close() {
      sock.close()
      if (ctx.channel.isOpen) ctx.channel.close()
    }

    val awaitAck = ack match {
      case Some(ackOffer) =>
        ackOffer {
          case Return(_) => listenWebSocket(ctx, sock, promise, None)
          case Throw(_) => close()
        }

      case None =>
        Offer.choose(
          sock.messages {
            message =>
              val frame = new TextWebSocketFrame(message)
              val writeFuture = ctx.writeAndFlush(frame)
              listenWebSocket(ctx, sock, promise, Some(channelFutureToOffer(writeFuture)))
          },
          sock.binaryMessages {
            binary =>
              val frame = new BinaryWebSocketFrame(Unpooled.copiedBuffer(binary))
              val writeFuture = ctx.writeAndFlush(frame)
              listenWebSocket(ctx, sock, promise, Some(channelFutureToOffer(writeFuture)))
          })
    }
    awaitAck.sync()
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    super.channelInactive(ctx)
    closer.setDone()
  }

  override def userEventTriggered(ctx: ChannelHandlerContext, e: AnyRef): Unit = {
    e match {
      case _: IdleStateEvent => ctx.channel.close()
      case _ => super.userEventTriggered(ctx, e)
    }
  }
}

class WebSocketServerHandler extends WebSocketHandler {
  private[this] var handshaker: Option[WebSocketServerHandshaker] = None

  private def createServerHandshaker(req: FullHttpRequest): WebSocketServerHandshaker = {
    val scheme = if (req.getUri.startsWith("wss")) "wss" else "ws"
    val location = scheme + "://" + req.headers.get(HttpHeaders.Names.HOST) + "/"
    val wsFactory = new WebSocketServerHandshakerFactory(location, null, false)
    wsFactory.newHandshaker(req)
  }

  private def makeHandshake(ctx: ChannelHandlerContext, req: FullHttpRequest, h: WebSocketServerHandshaker): ChannelFuture = {
    h.handshake(ctx.channel, req).addListener(new ChannelFutureListener {
      override def operationComplete(future: ChannelFuture): Unit = {
        if (!future.isSuccess)
          ctx.fireExceptionCaught(future.cause)
      }
    })
  }

  private def triggerWebSocketReceive(ctx: ChannelHandlerContext, req: FullHttpRequest): ChannelHandlerContext = {
    def close(): ChannelFuture = ctx.channel().close()

    val webSocket = WebSocket(
      messages = messagesBroker.recv,
      binaryMessages = binaryMessagesBroker.recv,
      pings = pingsBroker.recv,
      uri = new URI(req.uri),
      headers = req.headers.entries.asScala.map(e => e.getKey -> e.getValue).toMap,
      remoteAddress = ctx.channel.remoteAddress,
      onClose = closer,
      close = close)

    ctx.fireChannelRead(webSocket)
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = {
    msg match {
      case req: FullHttpRequest =>
        try {
          handshaker = Option(createServerHandshaker(req))
          handshaker match {
            case None =>
              WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel)

            case Some(h) =>
              makeHandshake(ctx, req, h)
              triggerWebSocketReceive(ctx, req)
          }
        } finally {
          req.release
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

  override def write(ctx: ChannelHandlerContext, msg: AnyRef, promise: ChannelPromise): Unit = {
    msg match {
      case sock: WebSocket =>
        listenWebSocket(ctx, sock, promise)

      case _: HttpResponse =>
        ctx.write(msg, promise)

      case _: PongWebSocketFrame =>
        ctx.write(msg, promise)

      case _: CloseWebSocketFrame =>
        ctx.write(msg, promise)

      case invalid =>
        ctx.fireExceptionCaught(new IllegalArgumentException("invalid message \"%s\"".format(invalid)))
    }
  }
}

class WebSocketClientHandler extends WebSocketHandler {
  @volatile private[this] var handshaker: Option[WebSocketClientHandshaker] = None
  private[this] var keepAliveTask: Option[TimerTask] = None
  private[this] var socket : Option[WebSocket] = None

  private def makeHandshake(ctx: ChannelHandlerContext) = {
    def initiateKeepAlive(): Unit =
      if (socket.isDefined)
        for (interval <- socket.get.keepAlive) {
          keepAliveTask = Option(timer.schedule(interval) {
            ctx.writeAndFlush(new PingWebSocketFrame())
          })
        }
      else
        new IllegalStateException("WebSocketClientHandshaker is not assigned with WebSocket")

    handshaker match {
      case None =>
        new IllegalStateException("WebSocketClientHandshaker is not assigned with handsaker")
      case Some(h) =>
        h.handshake(ctx.channel).addListener(new ChannelFutureListener {
          override def operationComplete(future: ChannelFuture): Unit = {
            if (!future.isSuccess) {
              ctx.fireExceptionCaught(future.cause())
            }
            initiateKeepAlive()
          }
        })
    }
  }

  private def createClientHandshaker(sock: WebSocket): WebSocketClientHandshaker = {
    def toHttpHeaders(headers: Map[String, String]): DefaultHttpHeaders = {
      val httpHeaders = new DefaultHttpHeaders()
      for ((k, v) <- headers) httpHeaders.add(k, v)
      httpHeaders
    }

    WebSocketClientHandshakerFactory.newHandshaker(sock.uri, sock.version, null, false, toHttpHeaders(sock.headers))
  }

  private def finishHandshakeAndTriggerWebSocketReceive(ctx: ChannelHandlerContext, res: FullHttpResponse, hs: WebSocketClientHandshaker, sock: WebSocket) = {
    try {
      if (!hs.isHandshakeComplete) {
        hs.finishHandshake(ctx.channel, res)
        ctx.fireChannelRead(sock)
      }
    } finally {
      res.release
    }
  }

  private def copyWebSocket(ctx: ChannelHandlerContext, promise: ChannelPromise, sock: WebSocket): WebSocket = {
    def close() {
      keepAliveTask.foreach(_.close())
      ctx.channel().close()
    }

    sock.copy(
      messages = messagesBroker.recv,
      binaryMessages = binaryMessagesBroker.recv,
      pings = pingsBroker.recv,
      onClose = closer,
      close = close)
  }

  private def returnImmediatly(promise: ChannelPromise): ChannelPromise = promise.setSuccess()

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    super.channelActive(ctx)
    makeHandshake(ctx)
  }

  override def channelRead(ctx:ChannelHandlerContext, msg: AnyRef):Unit = {
    msg match {
      case res: FullHttpResponse if handshaker.isDefined && socket.isDefined =>
        finishHandshakeAndTriggerWebSocketReceive(ctx, res, handshaker.get, socket.get)

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
        handshaker = Some(createClientHandshaker(sock))
        socket = Some(copyWebSocket(ctx, promise, sock))
        listenWebSocket(ctx, sock, promise)
        returnImmediatly(promise)

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
