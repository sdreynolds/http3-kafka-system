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
import com.github.sdreynolds.streams.SubscriptionService;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.incubator.codec.http3.DefaultHttp3DataFrame;
import io.netty.incubator.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.incubator.codec.http3.Http3DataFrame;
import io.netty.incubator.codec.http3.Http3HeadersFrame;
import io.netty.incubator.codec.http3.Http3RequestStreamInboundHandler;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.event.ruler.JsonRuleCompiler;

public final class EventsHandler extends Http3RequestStreamInboundHandler implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(EventsHandler.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final SubscriptionService service;

  private final CountDownLatch latch = new CountDownLatch(1);

  private BlockingQueue<Map<String, Object>> eventQueue;
  private ChannelHandlerContext listenerCtx;

  EventsHandler(final SubscriptionService service) {
    this.service = service;
  }

  @Override
  protected void channelRead(ChannelHandlerContext ctx, Http3HeadersFrame frame, boolean isLast)
      throws Exception {
    final var requestedPath = frame.headers().path();
    LOGGER.info("HEadder read: {}", requestedPath);
    ReferenceCountUtil.release(frame);
  }

  @Override
  protected void channelRead(ChannelHandlerContext ctx, Http3DataFrame frame, boolean isLast)
      throws Exception {
    final var filter = frame.content().toString(Charset.defaultCharset());

    try {
      var compiledPatterns = JsonRuleCompiler.compile(filter);
      // @TODO: create the polling loop with threads to push out to ctx
      final var sub = service.subscribe(compiledPatterns);
      eventQueue = sub.queue();
      listenerCtx = ctx;

      ctx.channel().closeFuture().addListener(v -> {
              service.unsubscribe(sub);
              latch.countDown();
          });
    } catch (IOException compilationFailure) {
      LOGGER.error("failed to compile filter: {}", filter);
    }

    LOGGER.info("Content read {}", filter);
    writeResponse(ctx);

    ctx.channel().eventLoop().submit(this);

    ReferenceCountUtil.release(frame);
  }

  private void writeResponse(ChannelHandlerContext ctx) {
    LOGGER.info("Sending events and keeping connection open");
    Http3HeadersFrame headersFrame = new DefaultHttp3HeadersFrame();
    headersFrame.headers().status("200");
    headersFrame.headers().add("server", "netty");
    ctx.write(headersFrame);
  }

  private void flushEvent(final Map<String, Object> event)
      throws JsonProcessingException, InterruptedException {
    final var encodedBytes = MAPPER.writeValueAsBytes(event);
    LOGGER.info("Writing out {}", new String(encodedBytes));
    listenerCtx.writeAndFlush(new DefaultHttp3DataFrame(Unpooled.wrappedBuffer(encodedBytes)));
  }

  @Override
  public void run() {
    LOGGER.info("Running thread");
    while (latch.getCount() > 0) {
      try {
        final var event = eventQueue.poll(1500, TimeUnit.MILLISECONDS);
        try {
          if (event != null) {
            flushEvent(event);
          } else {
            LOGGER.info("Nothing in the event");
          }
        } catch (JsonProcessingException e) {
          LOGGER.error("Failed to process event into json {}", event, e);
        }
      } catch (InterruptedException e) {
        // @TODO: look this back up and make sure this is the right way to
        // handle
        // @TODO: should this call `close()`
        Thread.currentThread().interrupt();
      }
    }
  }
}
