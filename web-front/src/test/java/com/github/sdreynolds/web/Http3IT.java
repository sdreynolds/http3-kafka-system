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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.github.sdreynolds.streams.Subscription;
import com.github.sdreynolds.streams.SubscriptionService;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(MockitoExtension.class)
public final class Http3IT {
  private static final Logger LOGGER = LoggerFactory.getLogger(Http3IT.class);

  @Test
  void connectToSameServer(@Mock SubscriptionService service) throws Exception {
    final Map<String, Object> testEvent = Map.of("awesome", "yes");
    final CompletionStage<Map<String, Object>> pendingEvent;

    BlockingQueue<Map<String, Object>> subscriptionEvents = new LinkedBlockingQueue<>(List.of(testEvent));

    when(service.findHostForKey(anyString()))
            .thenReturn(CompletableFuture.completedStage(URI.create("/127.0.0.1:9090")));

    when(service.subscribe(anyList()))
            .thenAnswer(
                    rules -> new Subscription(UUID.randomUUID(), subscriptionEvents, rules.getArgument(0)));

    LOGGER.info("About to start event server and client");
    try (final var server = new EventServer(9090, service);
        final var client = new StreamWebClient("localhost", 9090, "123")) {

      LOGGER.info("Server and client started");

      pendingEvent =
          client
              .getEvents()
              .thenCompose(
                  queue -> {
                    final CompletableFuture<Map<String, Object>> testResult =
                        new CompletableFuture<>();
                    try {
                      LOGGER.info("Polling for events");
                      final var event = queue.poll(10000, TimeUnit.MILLISECONDS);
                      LOGGER.info("Event received: {}", event);
                      testResult.complete(event);
                    } catch (Exception ex) {
                      testResult.completeExceptionally(ex);
                    }

                    return testResult;
                  });
      final var event = pendingEvent.toCompletableFuture().get(1000, TimeUnit.MILLISECONDS);
      assertThat(event).containsExactlyEntriesOf(testEvent);
    }
    LOGGER.info("Closed all down");
  }
}
