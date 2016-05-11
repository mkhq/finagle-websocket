package com.twitter.finagle.websocket.examples

import com.twitter.app.App
import com.twitter.finagle.{Service, Websocket, ServiceFactory, ClientConnection}
import com.twitter.finagle.websocket._
import com.twitter.util.{Await, Future, Time}


object EchoServer
extends App {
  val bind = flag("bind", "127.0.0.1:10001", "Host and port where the server listens")

  def service = new Service[Request, Response] {
    def apply(req: Request): Future[Response] = {
      Future(Response(req.messages.map {
        case frame @ Frame.Text(data) =>
          println("Text: " + data)
          frame
        case frame @ Frame.Binary(buf) => frame
        case frame @ Frame.Ping(_) => frame
        case frame @ Frame.Pong(_) => frame
      }))
    }
  }

  val factory = new ServiceFactory[Request, Response] {
    def apply(conn: ClientConnection): Future[Service[Request, Response]] = {
      println(s"Accepted connection $conn")

      conn.onClose.map { _ => println(s"Closing $conn") }

      Future.value(service)
    }

    def close(deadline:Time) = Future.Done
  }

  def main() {
    val server = Websocket.server
      .withLabel("echo-server")
      .serve(bind(), factory)

    Await.ready(server)

    Await.ready(server.close())
  }

}
