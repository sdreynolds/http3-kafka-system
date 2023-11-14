/* Copyright 2023 Scott Reynolds
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.sdreynolds.web;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.incubator.codec.http3.DefaultHttp3DataFrame;
import io.netty.incubator.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.incubator.codec.http3.Http3DataFrame;
import io.netty.incubator.codec.http3.Http3HeadersFrame;
import io.netty.incubator.codec.http3.Http3RequestStreamInboundHandler;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.util.ReferenceCountUtil;

import java.util.concurrent.CompletionStage;

import com.github.sdreynolds.streams.SubscriptionService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ConnectionHandler extends Http3RequestStreamInboundHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionHandler.class);
  private String subscriptionId;
  private final SubscriptionService service;

    ConnectionHandler(final SubscriptionService service) {
        this.service = service;
    }

  @Override
  protected void channelRead(ChannelHandlerContext ctx, Http3HeadersFrame frame, boolean isLast) {
    subscriptionId = frame.headers().path().toString().substring(10);

    if (isLast) {
      writeResponse(ctx);
    }
    ReferenceCountUtil.release(frame);
  }

  @Override
  protected void channelRead(ChannelHandlerContext ctx, Http3DataFrame frame, boolean isLast) {
    if (isLast) {
      writeResponse(ctx);
    }
    ReferenceCountUtil.release(frame);
  }

  private CompletionStage<String> locateServer() {
      return service.findHostForKey(subscriptionId).thenApply(uri -> uri.toString());
  }

  private void writeResponse(ChannelHandlerContext ctx) {
      locateServer().whenComplete((location, failure) -> {
              if (failure != null) {
                  Http3HeadersFrame errorHeaders = new DefaultHttp3HeadersFrame();
                  errorHeaders.headers().status("500");
                  ctx.writeAndFlush(errorHeaders)
                      .addListener(QuicStreamChannel.SHUTDOWN_OUTPUT);

                  return;
              }
          LOGGER.info("Redirecting");
          final byte[] locationBytes = location.getBytes();
          Http3HeadersFrame headersFrame = new DefaultHttp3HeadersFrame();
          headersFrame.headers().status("302");
          headersFrame.headers().add("server", "netty");
          headersFrame.headers().addInt("content-length", locationBytes.length);
          ctx.write(headersFrame);
          ctx.writeAndFlush(new DefaultHttp3DataFrame(Unpooled.wrappedBuffer(locationBytes)))
                  .addListener(QuicStreamChannel.SHUTDOWN_OUTPUT);
      });
  }
}
