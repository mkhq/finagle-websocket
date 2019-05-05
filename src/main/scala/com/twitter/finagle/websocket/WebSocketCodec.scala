package com.twitter.finagle.websocket

import io.netty.channel.ChannelPipeline
import io.netty.handler.codec.http._
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketServerCompressionHandler

object WebSocketServerPipelineFactory extends (ChannelPipeline => Unit) {
  override def apply(pipeline: ChannelPipeline): Unit = {
    pipeline.addLast("httpCodec", new HttpServerCodec)
    pipeline.addLast("httpAggregator", new HttpObjectAggregator(65536))
    pipeline.addLast("wsCompressor", new WebSocketServerCompressionHandler())
    pipeline.addLast("handler", new WebSocketServerHandler)
  }
}

object WebSocketClientPipelineFactory extends (ChannelPipeline => Unit) {
  override def apply(pipeline: ChannelPipeline): Unit = {
    pipeline.addLast("httpCodec", new HttpClientCodec())
    pipeline.addLast("httpAggregator", new HttpObjectAggregator(8192))
    pipeline.addLast("handler", new WebSocketClientHandler)
  }
}
