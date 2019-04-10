package com.twitter.finagle.websocket

import com.twitter.finagle.{Codec, CodecFactory}
import org.jboss.netty.channel.{ChannelPipelineFactory, Channels}
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.handler.timeout.IdleStateHandler
import org.jboss.netty.util.HashedWheelTimer

case class WebSocketCodec(sessionIdleTimeSeconds: Int = 0) extends CodecFactory[WebSocket, WebSocket] {
  def server = Function.const {
    new Codec[WebSocket, WebSocket] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("decoder", new HttpRequestDecoder)
          pipeline.addLast("encoder", new HttpResponseEncoder)
          pipeline.addLast("idleStateHandler",  new IdleStateHandler(new HashedWheelTimer(), 0, 0, sessionIdleTimeSeconds))
          pipeline.addLast("handler", new WebSocketServerHandler)
          pipeline
        }
      }
    }
  }

  def client = Function.const {
    new Codec[WebSocket, WebSocket] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("decoder", new HttpResponseDecoder)
          pipeline.addLast("encoder", new HttpRequestEncoder)
          pipeline.addLast("handler", new WebSocketClientHandler)
          pipeline
        }
      }
    }
  }
}
