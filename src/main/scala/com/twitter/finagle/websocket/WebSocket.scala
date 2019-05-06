package com.twitter.finagle.websocket

import java.net.{SocketAddress, URI}

import com.twitter.concurrent.Offer
import com.twitter.util.{Duration, Future, Promise}
import io.netty.handler.codec.http.websocketx.WebSocketVersion

case class WebSocket(
  messages: Offer[String],
  binaryMessages: Offer[Array[Byte]],
  pings: Offer[Array[Byte]],
  uri: URI,
  headers: Map[String, String] = Map.empty[String, String],
  remoteAddress: SocketAddress = new SocketAddress {},
  version: WebSocketVersion = WebSocketVersion.V13,
  onClose: Future[Unit] = new Promise[Unit],
  close: () => Unit = { () => () },
  keepAlive: Option[Duration] = None)
