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

import com.github.sdreynolds.streams.SubscriptionService;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.incubator.codec.http3.Http3;
import io.netty.incubator.codec.http3.Http3ServerConnectionHandler;
import io.netty.incubator.codec.quic.InsecureQuicTokenHandler;
import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.util.CharsetUtil;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class EventServer implements AutoCloseable {
  private Map<String, BlockingQueue<Map<String, Object>>> eventQueues =
      Map.of("t", new LinkedBlockingQueue<>());
  private static final byte[] CONTENT = "Hello World!\r\n".getBytes(CharsetUtil.US_ASCII);
  private static final Logger LOGGER = LoggerFactory.getLogger(EventServer.class);

  final EventLoopGroup locationGroup;
  final EventLoopGroup eventGroup;
  final Channel locationChannel;
  final Channel eventsChannel;

  final Bootstrap locationBootstrap;

  public EventServer(final int port, final SubscriptionService service) throws Exception {
    locationGroup = new NioEventLoopGroup(1);
    eventGroup = new NioEventLoopGroup(1);
    SelfSignedCertificate cert = new SelfSignedCertificate();
    QuicSslContext sslContext =
        QuicSslContextBuilder.forServer(cert.key(), null, cert.cert())
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();
    ChannelHandler locationCodec =
        Http3.newQuicServerCodecBuilder()
            .sslContext(sslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    LOGGER.info("Init Channel Location");
                    // Called for each connection
                    ch.pipeline()
                        .addLast(
                            new Http3ServerConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  // Called for each request-stream,
                                  @Override
                                  protected void initChannel(QuicStreamChannel ch) {
                                    ch.pipeline().addLast(new ConnectionHandler());
                                  }
                                }));
                  }
                })
            .build();

    ChannelHandler eventCodec =
        Http3.newQuicServerCodecBuilder()
            .sslContext(sslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    LOGGER.info("Init Channel Event");
                    // Called for each connection
                    ch.pipeline()
                        .addLast(
                            new Http3ServerConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  // Called for each request-stream,
                                  @Override
                                  protected void initChannel(QuicStreamChannel ch) {
                                    ch.pipeline().addLast(new EventsHandler(service));
                                  }
                                }));
                  }
                })
            .build();
    locationBootstrap = new Bootstrap();
    locationChannel =
        locationBootstrap
            .group(locationGroup)
            .channel(NioDatagramChannel.class)
            .handler(locationCodec)
            .bind(new InetSocketAddress(port))
            .sync()
            .channel();
    Bootstrap eventBootstrap = new Bootstrap();
    eventsChannel =
        eventBootstrap
            .group(eventGroup)
            .channel(NioDatagramChannel.class)
            .handler(eventCodec)
            .bind(new InetSocketAddress(4056))
            .sync()
            .channel();
  }

  public BlockingQueue<Map<String, Object>> getConnectionQueue() {
    return eventQueues.get("t");
  }

  @Override
  public void close() throws Exception {
    LOGGER.info("Starting close");
    locationChannel.closeFuture().await(3, TimeUnit.SECONDS);
    LOGGER.info("Location channel closed");
    locationGroup.shutdownGracefully(3, 5, TimeUnit.SECONDS);
    LOGGER.info("events channel starting");
    eventsChannel.closeFuture().await(3, TimeUnit.SECONDS);
    eventGroup.shutdownGracefully(3, 5, TimeUnit.SECONDS);
    LOGGER.info("Ending close");
  }
}
