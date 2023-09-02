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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.incubator.codec.http3.DefaultHttp3DataFrame;
import io.netty.incubator.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.incubator.codec.http3.Http3DataFrame;
import io.netty.incubator.codec.http3.Http3HeadersFrame;
import io.netty.incubator.codec.http3.Http3RequestStreamInboundHandler;
import io.netty.util.ReferenceCountUtil;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class EventsHandler extends Http3RequestStreamInboundHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(EventsHandler.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  protected void channelRead(ChannelHandlerContext ctx, Http3HeadersFrame frame, boolean isLast)
      throws Exception {
    LOGGER.info("HEadder read");
    writeResponse(ctx);
    ReferenceCountUtil.release(frame);
  }

  @Override
  protected void channelRead(ChannelHandlerContext ctx, Http3DataFrame frame, boolean isLast)
      throws Exception {
    LOGGER.info("Content read");
    ReferenceCountUtil.release(frame);
  }

  private void writeResponse(ChannelHandlerContext ctx) {
    LOGGER.info("Sending events and keeping connection open");
    Http3HeadersFrame headersFrame = new DefaultHttp3HeadersFrame();
    headersFrame.headers().status("200");
    headersFrame.headers().add("server", "netty");
    ctx.write(headersFrame);

    final Map<String, Object> event = Map.of("awesome", "yes");
    try {
      flushEvent(ctx, event);
    } catch (Exception e) {
      LOGGER.error("Failed to translated {} into json", event, e);
    }
  }

  private void flushEvent(final ChannelHandlerContext ctx, final Map<String, Object> event)
      throws JsonProcessingException, InterruptedException {
    final var encodedBytes = MAPPER.writeValueAsBytes(event);
    LOGGER.info("Writing out {}", new String(encodedBytes));
    ctx.writeAndFlush(new DefaultHttp3DataFrame(Unpooled.wrappedBuffer(encodedBytes)));
  }
}