package com.twitter.finagle.websocket.examples

import com.twitter.app.App
import com.twitter.concurrent.AsyncStream
import com.twitter.finagle.{Service, Websocket}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.websocket._
import com.twitter.util.{Await, Future, FuturePool, Promise}
import java.net.{URI, SocketAddress}
import io.StdIn

object ConsoleClient
extends App {
  import com.twitter.conversions.time._
  implicit val timer = DefaultTimer.twitter

  val server = flag("server", "127.0.0.1:10001", "Host and port where the server listens")
  val pool = FuturePool.unboundedPool

  def readLine: AsyncStream[String] = {
    val textF = pool { StdIn.readLine }
      .flatMap { text =>
        if(text == null || text.isEmpty)
          Future.exception(new Exception("End of Stream"))
        else Future.value(text)
      }

      AsyncStream.fromFuture(textF) ++ readLine
  }

  def main() {
    val client: Service[Request, Response] = Websocket.client
      .newService(server(), "console-client")

    val input: AsyncStream[Frame] = readLine.map(Frame.Text(_))

    val req = Request(new URI("/"), Map.empty, new SocketAddress{}, input)

    val respF: Future[Response] = client(req)

    respF.map { resp =>
      resp.messages.foreach {
        msg => println(s"Echo $msg")
      }

      resp
    }

    Await.ready(input.force)

    client.close()
  }

}
