package com.twitter.finagle.websocket

import java.net.URI

import com.twitter.concurrent.{Broker, Offer}
import com.twitter.finagle.{CancelledRequestException, MaxContentLength}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util._
import io.netty.buffer.{ByteBufUtil, Unpooled}
import io.netty.channel._
import io.netty.handler.codec.http._
import io.netty.handler.codec.http.websocketx._
import io.netty.handler.timeout.IdleStateEvent
import io.netty.util.ReferenceCountUtil

import scala.collection.JavaConverters._

class WebSocketHandler extends ChannelDuplexHandler {
  protected[this] val messagesBroker = new Broker[String]
  protected[this] val binaryMessagesBroker = new Broker[Array[Byte]]
  protected[this] val pingsBroker = new Broker[Array[Byte]]
  protected[this] val closer = new Promise[Unit]
  protected[this] val timer = DefaultTimer.twitter

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

class WebSocketServerHandler(maxFramePayloadLength: Int = MaxContentLength.DefaultMaxLengthBytes) extends WebSocketHandler {
  private[this] var handshaker: Option[WebSocketServerHandshaker] = None

  private def createServerHandshaker(req: FullHttpRequest): WebSocketServerHandshaker = {
    val scheme = if (req.getUri.startsWith("wss")) "wss" else "ws"
    val location = scheme + "://" + req.headers.get(HttpHeaders.Names.HOST) + "/"
    val wsFactory = new WebSocketServerHandshakerFactory(location, null, false, maxFramePayloadLength)
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
    try {
      msg match {
        case req: FullHttpRequest =>
          handshaker = Option(createServerHandshaker(req))
          handshaker match {
            case None =>
              WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel)

            case Some(h) =>
              makeHandshake(ctx, req, h)
              triggerWebSocketReceive(ctx, req)
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

          pingsBroker ! ByteBufUtil.getBytes(frameContent)
          ctx.writeAndFlush(new PongWebSocketFrame(frameContent))

        case frame: TextWebSocketFrame =>
          messagesBroker ! frame.text

        case frame: BinaryWebSocketFrame =>
          binaryMessagesBroker ! ByteBufUtil.getBytes(frame.content)

        case invalid =>
          ctx.fireExceptionCaught(new IllegalArgumentException("invalid message \"%s\"".format(invalid)))
      }
    } finally ReferenceCountUtil.release(msg)
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

class WebSocketClientHandler(maxFramePayloadLength: Int = MaxContentLength.DefaultMaxLengthBytes) extends WebSocketHandler {
  @volatile private[this] var handshaker: Option[WebSocketClientHandshaker] = None
  private[this] var keepAliveTask: Option[TimerTask] = None
  private[this] var socket : Option[WebSocket] = None

  private def createClientHandshaker(ctx: ChannelHandlerContext, sock: WebSocket): WebSocketClientHandshaker =
    WebSocketClientHandshakerFactory.newHandshaker(sock.uri, sock.version, null, false, new DefaultHttpHeaders(), maxFramePayloadLength)

  private def makeHandshake(ctx: ChannelHandlerContext, h: WebSocketClientHandshaker, sock: WebSocket): ChannelFuture = {
    def initiateKeepAlive(): Unit =
      for (interval <- sock.keepAlive) {
        keepAliveTask = Option(timer.schedule(interval) {
          ctx.writeAndFlush(new PingWebSocketFrame())
        })
      }

    h.handshake(ctx.channel).addListener(new ChannelFutureListener {
      override def operationComplete(future: ChannelFuture): Unit = {
        if (!future.isSuccess) {
          ctx.fireExceptionCaught(future.cause())
        }
        initiateKeepAlive()
      }
    })
  }

  private def finishHandshakeAndTriggerWebSocketReceive(ctx: ChannelHandlerContext, res: FullHttpResponse, hs: WebSocketClientHandshaker, sock: WebSocket) = {
    if (!hs.isHandshakeComplete) {
      hs.finishHandshake(ctx.channel, res)
      ctx.fireChannelRead(sock)
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

  private def returnImmediately(promise: ChannelPromise): ChannelPromise = promise.setSuccess()

  override def channelRead(ctx:ChannelHandlerContext, msg: AnyRef):Unit = {
    try {
      msg match {
        case res: FullHttpResponse if handshaker.isDefined && socket.isDefined =>
          finishHandshakeAndTriggerWebSocketReceive(ctx, res, handshaker.get, socket.get)

        case frame: CloseWebSocketFrame =>
          ctx.channel.close()

        case frame: PongWebSocketFrame =>
        // Ack for the PingWebSocketFrame, do nothing

        case frame: TextWebSocketFrame =>
          messagesBroker ! frame.text

        case frame: BinaryWebSocketFrame =>
          binaryMessagesBroker ! ByteBufUtil.getBytes(frame.content)

        case invalid =>
          ctx.fireExceptionCaught(new IllegalArgumentException("invalid message \"%s\"".format(invalid)))
      }
    } finally ReferenceCountUtil.release(msg)
  }

  override def write(ctx: ChannelHandlerContext, e: AnyRef, promise: ChannelPromise): Unit = {
    e match {
      case sock: WebSocket =>
        socket = Some(copyWebSocket(ctx, promise, sock))
        handshaker = Some(createClientHandshaker(ctx, sock))
        handshaker match {
          case None =>
            ctx.fireExceptionCaught(new IllegalArgumentException("invalid websocket message \"%s\"".format(sock)))

          case Some(h) =>
            makeHandshake(ctx, h, sock)
        }

        listenWebSocket(ctx, sock, promise)
        returnImmediately(promise)

      case httpRequest: HttpRequest =>
        def updateHeaders(httpRequest: HttpRequest): HttpRequest = {
          for {
            sock <- socket
            (k, v) <- sock.headers
          } httpRequest.headers().add(k, v)

          httpRequest
        }

        ctx.write(updateHeaders(httpRequest), promise)

      case _: CloseWebSocketFrame =>
        ctx.write(e, promise)

      case _: PingWebSocketFrame =>
        ctx.write(e, promise)

      case invalid =>
        ctx.fireExceptionCaught(new IllegalArgumentException("invalid message \"%s\"".format(invalid)))
    }
  }
}
