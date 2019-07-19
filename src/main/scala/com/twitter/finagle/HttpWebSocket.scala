package com.twitter.finagle

import java.net.{SocketAddress, URI}

import com.twitter.concurrent.Offer
import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.{SerialClientDispatcher, SerialServerDispatcher}
import com.twitter.finagle.netty4.{Netty4Listener, Netty4Transporter}
import com.twitter.finagle.param.{Label, ProtocolLibrary, Stats}
import com.twitter.finagle.server._
import com.twitter.finagle.ssl.TrustCredentials
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.finagle.websocket._
import com.twitter.util.{Duration, Future}
import io.netty.channel.ChannelPipeline
import io.netty.handler.codec.http.{HttpClientCodec, HttpObjectAggregator, HttpServerCodec}
import io.netty.handler.ssl.SslContext
import io.netty.handler.timeout.IdleStateHandler

trait WebSocketRichClient { self: Client[WebSocket, WebSocket] =>
  def open(out: Offer[String], uri: String): Future[WebSocket] =
    open(out, Offer.never,  Offer.never, new URI(uri))

  def open(out: Offer[String], uri: URI): Future[WebSocket] =
    open(out, Offer.never, Offer.never, uri)

  def open(out: Offer[String], binaryOut: Offer[Array[Byte]], uri: String): Future[WebSocket] =
    open(out, binaryOut, Offer.never, new URI(uri))

  def open(out: Offer[String], binaryOut: Offer[Array[Byte]], uri: String,
           keepAlive: Option[Duration]): Future[WebSocket] =
    open(out, binaryOut, Offer.never, new URI(uri), keepAlive = keepAlive)

  def open(out: Offer[String], binaryOut: Offer[Array[Byte]], pingOut: Offer[Array[Byte]],
           uri: URI, keepAlive: Option[Duration] = None): Future[WebSocket] = {
    val socket = WebSocket(
      messages = out,
      binaryMessages = binaryOut,
      pings = pingOut,
      uri = uri,
      keepAlive = keepAlive)
    val addr = uri.getHost + ":" + uri.getPort

    var cli = HttpWebSocket.client

    if(uri.getScheme == "wss")
      cli = cli.withTlsWithoutValidation()

    cli.newClient(addr).toService(socket)
  }
}

object MaxContentLength {
  val DefaultMaxLengthBytes = 65536
  implicit val param: Stack.Param[MaxContentLength] =
    Stack.Param(MaxContentLength(DefaultMaxLengthBytes))
}

case class MaxContentLength(bytes: Int) {
  def mk(): (MaxContentLength, Stack.Param[MaxContentLength]) =
    (this, MaxContentLength.param)
}

object WebSocketClient {
  protected var sslCtx: Option[SslContext] = None
  val stack: Stack[ServiceFactory[WebSocket, WebSocket]] =
    StackClient.newStack
}

case class WebSocketClient(stack: Stack[ServiceFactory[WebSocket, WebSocket]] = WebSocketClient.stack,
                           params: Stack.Params = StackClient.defaultParams + ProtocolLibrary("websocket"))
  extends StdStackClient[WebSocket, WebSocket, WebSocketClient] {
  protected type In = WebSocket
  protected type Out = WebSocket
  protected type Context = TransportContext

  protected def newTransporter(addr: SocketAddress): Transporter[WebSocket, WebSocket, TransportContext] = {
    Netty4Transporter.raw((pipeline: ChannelPipeline) => {
      pipeline.addLast("httpCodec", new HttpClientCodec())
      pipeline.addLast("httpAggregator", new HttpObjectAggregator(params[MaxContentLength].bytes))
      pipeline.addLast("handler", new WebSocketClientHandler(params[MaxContentLength].bytes))
    }, addr, params)
  }

  protected def copy1(stack: Stack[ServiceFactory[WebSocket, WebSocket]] = this.stack,
                      params: Stack.Params = this.params): WebSocketClient = copy(stack, params)

  protected def newDispatcher(transport: Transport[WebSocket, WebSocket]
    {type Context <: WebSocketClient.this.Context}): Service[WebSocket, WebSocket] = new SerialClientDispatcher(transport)

  def withTlsWithoutValidation(): WebSocketClient =
    configured(Transport.ClientSsl(Some(SslClientConfiguration(trustCredentials = TrustCredentials.Insecure))))

  def withMaxContentLength(maxContentLengthBytes: Int): WebSocketClient = configured(MaxContentLength(maxContentLengthBytes))
}

object SessionIdleTimeout {
  implicit val param: Stack.Param[SessionIdleTimeout] =
    Stack.Param(SessionIdleTimeout(0))
}

case class SessionIdleTimeout(seconds: Int) {
  def mk(): (SessionIdleTimeout, Stack.Param[SessionIdleTimeout]) =
    (this, SessionIdleTimeout.param)
}

object WebSocketServer {
  val stack: Stack[ServiceFactory[WebSocket, WebSocket]] = StackServer.newStack
}

case class WebSocketServer(stack: Stack[ServiceFactory[WebSocket, WebSocket]] = WebSocketServer.stack,
                           params: Stack.Params = StackServer.defaultParams + ProtocolLibrary("websocket"),
                           sessionIdleTimeout: Int = 0) extends StdStackServer[WebSocket, WebSocket, WebSocketServer] {
  protected type In = WebSocket
  protected type Out = WebSocket
  protected type Context = TransportContext

  protected def newListener(): Listener[WebSocket, WebSocket, TransportContext] = {
    Netty4Listener((pipeline: ChannelPipeline) => {
      pipeline.addLast("httpCodec", new HttpServerCodec)
      pipeline.addLast("httpAggregator", new HttpObjectAggregator(params[MaxContentLength].bytes))

      if(params[SessionIdleTimeout].seconds > 0)
        pipeline.addLast("idleStateHandler",  new IdleStateHandler(0, 0, params[SessionIdleTimeout].seconds))

      pipeline.addLast("handler", new WebSocketServerHandler(params[MaxContentLength].bytes))
    }, params)
  }

  protected def newDispatcher(transport: Transport[WebSocket, WebSocket] { type Context <: WebSocketServer.this.Context },
                              service: Service[WebSocket, WebSocket]
                             ): SerialServerDispatcher[WebSocket, WebSocket] = {
    val Stats(_) = params[Stats]
    new SerialServerDispatcher(transport, service)
  }

  protected def copy1(stack: Stack[ServiceFactory[WebSocket, WebSocket]] = this.stack,
                      params: Stack.Params = this.params): WebSocketServer = copy(stack, params)

  def withTls(config: SslServerConfiguration): WebSocketServer = configured(Transport.ServerSsl(Some(config)))

  def withSessionIdleTimeout(seconds: Int): WebSocketServer = configured(SessionIdleTimeout(seconds))

  def withMaxContentLength(maxContentLengthBytes: Int): WebSocketServer = configured(MaxContentLength(maxContentLengthBytes))
}

object HttpWebSocket
extends Client[WebSocket, WebSocket]
with Server[WebSocket, WebSocket]
with WebSocketRichClient {
  val client: WebSocketClient = WebSocketClient().configured(Label("websocket"))
  val server: WebSocketServer = WebSocketServer().configured(Label("websocket"))

  def newClient(dest: Name, label: String): ServiceFactory[WebSocket, WebSocket] =
    client.newClient(dest, label)

  def newService(dest: Name, label: String): Service[WebSocket, WebSocket] =
    client.newService(dest, label)

  def serve(addr: SocketAddress, service: ServiceFactory[WebSocket, WebSocket]): ListeningServer =
    server.serve(addr, service)
}
