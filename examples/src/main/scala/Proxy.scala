package com.twitter.finagle.websocket.examples

import com.twitter.app.App
import com.twitter.finagle.{Service, Websocket, ServiceFactory, ClientConnection}
import com.twitter.finagle.websocket._
import com.twitter.util.{Await, Future, Time}


object ProxyServer
extends App {
  val bind = flag("bind", "127.0.0.1:10010", "Host and port where the proxy listens")
  val server = flag("server", "127.0.0.1:10001", "Host and port where the server listens")

  class ProxyService(client:Service[Request, Response])
  extends Service[Request, Response] {
    def apply(req: Request): Future[Response] = {
      // connect the messages coming from the request with the messages written
      // to the client and vice versa, i.e. we forward the request
      client(req)
    }
  }

  val factory = new ServiceFactory[Request, Response] {
    def apply(conn: ClientConnection): Future[Service[Request, Response]] = {
      println(s"Accepted proxy session $conn")
      val client: Service[Request, Response] = Websocket.client
        .newService(server(), "proxy-client")

      conn.onClose
        .flatMap { _ => client.close() }
        .onSuccess { _ => println(s"Closed roxy session $conn") }

      Future.value(new ProxyService(client))
    }

    def close(deadline:Time) = Future.Done
  }

  def main() {
    val server = Websocket.server
      .withLabel("proxy-server")
      .serve(bind(), factory)

    Await.ready(server)

    Await.ready(server.close())
  }

}
