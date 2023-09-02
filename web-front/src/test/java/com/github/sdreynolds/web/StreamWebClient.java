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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.incubator.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.incubator.codec.http3.Http3;
import io.netty.incubator.codec.http3.Http3ClientConnectionHandler;
import io.netty.incubator.codec.http3.Http3DataFrame;
import io.netty.incubator.codec.http3.Http3HeadersFrame;
import io.netty.incubator.codec.http3.Http3RequestStreamFrame;
import io.netty.incubator.codec.http3.Http3RequestStreamInboundHandler;
import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StreamWebClient implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamWebClient.class);
  final Channel channel;
  final QuicChannel quicChannel;
  final EventLoopGroup group;

  final CompletionStage<BlockingQueue<Map<String, Object>>> eventQueue;

  StreamWebClient(final String serverAddress, final int port, final String primaryId)
      throws Exception {
    // @TODO: this could be epoll or kqueue or io_uring instead of native java
    group = new NioEventLoopGroup(1);

    QuicSslContext context =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();
    ChannelHandler codec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(context)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .build();

    Bootstrap bs = new Bootstrap();
    channel =
        bs.group(group).channel(NioDatagramChannel.class).handler(codec).bind(0).sync().channel();

    LOGGER.info("About to create first connection");

    quicChannel =
        QuicChannel.newBootstrap(channel)
            .handler(new Http3ClientConnectionHandler())
            .remoteAddress(new InetSocketAddress(serverAddress, port))
            .connect()
            .get();

    LOGGER.info("About to request location");
    final CompletableFuture<InetSocketAddress> serverLocation = new CompletableFuture<>();
    QuicStreamChannel streamChannel =
        Http3.newRequestStream(
                quicChannel,
                new Http3RequestStreamInboundHandler() {
                  @Override
                  protected void channelRead(
                      ChannelHandlerContext ctx, Http3HeadersFrame frame, boolean isLast) {
                    LOGGER.info("Headers are {}", frame);
                    releaseFrameAndCloseIfLast(ctx, frame, isLast);
                  }

                  @Override
                  protected void channelRead(
                      ChannelHandlerContext ctx, Http3DataFrame frame, boolean isLast) {
                    final String responseString = frame.content().toString(CharsetUtil.US_ASCII);
                    if (frame.content().maxCapacity() > 0) {
                      LOGGER.info("Response is {}", frame);
                      final var responseSplit = responseString.split(":");
                      LOGGER.info(
                          "Target for events is {}",
                          new InetSocketAddress(
                              responseSplit[0], Integer.parseInt(responseSplit[1])));
                      serverLocation.complete(
                          new InetSocketAddress(
                              responseSplit[0], Integer.parseInt(responseSplit[1])));
                    }
                    releaseFrameAndCloseIfLast(ctx, frame, isLast);
                  }

                  private void releaseFrameAndCloseIfLast(
                      ChannelHandlerContext ctx, Http3RequestStreamFrame frame, boolean isLast) {
                    ReferenceCountUtil.release(frame);
                    if (isLast) {
                      ctx.close();
                    }
                  }
                })
            .sync()
            .getNow();

    LOGGER.info("About to write for the primiary ID");
    // Write the Header frame and send the FIN to mark the end of the request.
    // After this its not possible anymore to write any more data.
    Http3HeadersFrame frame = new DefaultHttp3HeadersFrame();
    frame
        .headers()
        .method("GET")
        .path(String.format("/location/%s", primaryId))
        .authority(String.format("%s:%d", serverAddress, port))
        .scheme("https");
    streamChannel.writeAndFlush(frame).addListener(QuicStreamChannel.SHUTDOWN_OUTPUT).sync();

    // Wait for the stream channel and quic channel to be closed (this will happen after we received
    // the FIN).
    // After this is done we will close the underlying datagram channel.
    streamChannel.closeFuture().sync();

    eventQueue =
        serverLocation
            .thenCompose(
                streamHost -> {
                  final CompletableFuture<QuicChannel> connected = new CompletableFuture<>();
                  final Future<QuicChannel> futureChannel =
                      QuicChannel.newBootstrap(channel)
                          .handler(new Http3ClientConnectionHandler())
                          .remoteAddress(streamHost)
                          .connect();

                  // @TODO: figure out what the type system is trying tell me about generic handler
                  try {
                    connected.complete(futureChannel.sync().get());
                  } catch (InterruptedException | ExecutionException e) {
                    connected.completeExceptionally(e);
                  }

                  return connected;
                })
            .thenCompose(
                channel -> {
                  final LinkedBlockingQueue<Map<String, Object>> events =
                      new LinkedBlockingQueue<>();

                  final var eventStream =
                      Http3.newRequestStream(
                          channel,
                          new Http3RequestStreamInboundHandler() {
                            @Override
                            protected void channelRead(
                                ChannelHandlerContext ctx,
                                Http3HeadersFrame frame,
                                boolean isLast) {
                              releaseFrameAndCloseIfLast(ctx, frame, isLast);
                            }

                            @Override
                            protected void channelRead(
                                ChannelHandlerContext ctx, Http3DataFrame frame, boolean isLast) {
                              final String responseString =
                                  frame.content().toString(CharsetUtil.US_ASCII);
                              LOGGER.info("Received a response {}", responseString);

                              final var mapper = new ObjectMapper();
                              try {
                                if (!responseString.isEmpty()) {
                                  final Map<String, Object> event =
                                      mapper.readValue(responseString, Map.class);
                                  events.offer(event);
                                }
                              } catch (Exception failedParse) {
                                LOGGER.error("Failed to parse", failedParse);
                              } finally {
                                releaseFrameAndCloseIfLast(ctx, frame, isLast);
                              }
                            }

                            private void releaseFrameAndCloseIfLast(
                                ChannelHandlerContext ctx,
                                Http3RequestStreamFrame frame,
                                boolean isLast) {
                              ReferenceCountUtil.release(frame);
                              if (isLast) {
                                ctx.close();
                              }
                            }
                          });
                  LOGGER.info("Starting new connection");
                  final var eventFrame = new DefaultHttp3HeadersFrame();
                  eventFrame
                      .headers()
                      .method("GET")
                      .path(String.format("/events/%s", primaryId))
                      .authority(serverLocation.toCompletableFuture().join().getHostString())
                      .scheme("https");

                  final CompletableFuture<BlockingQueue<Map<String, Object>>> connected =
                      new CompletableFuture<>();
                  eventStream.addListener(
                      connectedStream -> {
                        if (connectedStream.isSuccess()) {
                          ((QuicStreamChannel) connectedStream.getNow()).writeAndFlush(eventFrame);
                          connected.complete(events);
                        } else {
                          connected.completeExceptionally(connectedStream.exceptionNow());
                        }
                      });
                  return connected;
                });
  }

  @Override
  public void close() throws Exception {
    quicChannel.close().sync();
    channel.close().sync();
    group.shutdownGracefully(30, 300, TimeUnit.MILLISECONDS);
  }

  public CompletionStage<BlockingQueue<Map<String, Object>>> getEvents() {
    return eventQueue;
  }
}
