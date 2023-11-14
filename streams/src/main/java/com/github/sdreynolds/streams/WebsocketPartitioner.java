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
package com.github.sdreynolds.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sdreynolds.streams.serde.JacksonSerde;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.event.ruler.GenericMachine;
import software.amazon.event.ruler.JsonRuleCompiler;
import software.amazon.event.ruler.Patterns;

public final class WebsocketPartitioner
    implements AutoCloseable, SubscriptionService, KafkaStreams.StateListener {
  private final Properties settings = new Properties();
  private final GenericMachine<UUID> rulesMachine = new GenericMachine<>();
  private final ConcurrentHashMap<UUID, Subscription> rules = new ConcurrentHashMap<>();
  private final KafkaStreams streamApplication;
  private final AdminClient admin;
  private final int rpcPort;
  private final CountDownLatch started = new CountDownLatch(1);

  private static Logger logger = LoggerFactory.getLogger(WebsocketPartitioner.class);
  private static ObjectMapper MAPPER = new ObjectMapper();

  public WebsocketPartitioner(
      String bootstrapServers,
      final String applicationName,
      final String topicName,
      final String jsonPartitionPath,
      final int rpcPort)
      throws InterruptedException {

    // Set a few key parameters
    settings.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName);
    settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    final var builder = new StreamsBuilder();

    builder.stream(topicName, Consumed.with(Serdes.Bytes(), new JacksonSerde<>(MAPPER, Map.class)))
        .filter(
            (unknownKey, jsonValue) -> {
              final var maybeKey = jsonValue.get(jsonPartitionPath);
              if (maybeKey == null) {
                logger.error(
                    "Event {} doesn't have a matching key path \"{}\"",
                    jsonValue,
                    jsonPartitionPath);
              }
              return maybeKey != null;
            })
        // The filter above ensures the key is there and is not null
        .selectKey((keyBytes, jsonValue) -> jsonValue.get(jsonPartitionPath).toString().getBytes())
        .repartition(
            Repartitioned.with(Serdes.ByteArray(), new JacksonSerde<>(MAPPER, Map.class))
                .withName("keyed-events"))
        .foreach(
            (newKey, event) -> {
              final var jacksonEvent = MAPPER.convertValue(event, JsonNode.class);
              final var matchingSubIds = rulesMachine.rulesForJSONEvent(jacksonEvent);
              matchingSubIds.forEach(
                  subId -> {
                    final var subscription = rules.get(subId);
                    if (subscription != null) {
                      if (subscription.queue().offer(event)) {
                        logger.info("Sent event to {}", subId);
                      }
                    }
                  });
            });
    streamApplication = new KafkaStreams(builder.build(), settings);
    streamApplication.setStateListener(this);
    streamApplication.start();

    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    admin = AdminClient.create(props);

    this.rpcPort = rpcPort;
    if (!started.await(5_000, TimeUnit.MILLISECONDS)) {
      throw new RuntimeException("Failed to start streams");
    }
  }

  @Override
  public void close() {
    streamApplication
        .metadataForAllStreamsClients()
        .forEach(metadata -> logger.info("names are {}", metadata.stateStoreNames()));

    streamApplication.close(Duration.ofMinutes(2));
    rules.forEachValue(0, sub -> unsubscribe(sub));
    admin.close(Duration.ofMinutes(2));
  }

  @Override
  public Subscription subscribe(final String rule) throws IOException {
    var compiledPatterns = JsonRuleCompiler.compile(rule);
    return subscribe(compiledPatterns);
  }

  @Override
  public Subscription subscribe(final List<Map<String, List<Patterns>>> compiledPatterns) {
    final var ruleId = UUID.randomUUID();
    final LinkedBlockingQueue<Map<String, Object>> queue = new LinkedBlockingQueue<>();
    final var sub = new Subscription(ruleId, queue, compiledPatterns);
    this.rules.put(ruleId, sub);
    compiledPatterns.forEach(r -> rulesMachine.addPatternRule(ruleId, r));
    return sub;
  }

  @Override
  public void unsubscribe(final Subscription sub) {
    sub.rules().forEach(r -> rulesMachine.deletePatternRule(sub.id(), r));
    // @TODO: assert sub.queue() == rules.remove(sub.id())
    rules.remove(sub.id());
    sub.queue().clear();
  }

  @Override
  public CompletionStage<URI> findHostForKey(String key) {
    final CompletableFuture<URI> discoveredURI = new CompletableFuture<>();

    final var pendingPartition =
        admin
            .describeTopics(List.of("events"))
            .topicNameValues()
            .get("events")
            .toCompletionStage()
            .thenApply(
                description ->
                    BuiltInPartitioner.partitionForKey(
                        key.getBytes(), description.partitions().size()))
            .toCompletableFuture();

    final var consumerGroup =
        admin
            .describeConsumerGroups(List.of("basic-test"))
            .describedGroups()
            .get("basic-test")
            .toCompletionStage()
            .toCompletableFuture();

    CompletableFuture.allOf(pendingPartition, consumerGroup)
        .whenComplete(
            (nil, failure) -> {
              if (failure != null) {
                discoveredURI.completeExceptionally(failure);
                return;
              }

              final var optionalURI =
                  consumerGroup.toCompletableFuture().join().members().stream()
                      .filter(
                          member ->
                              member.assignment().topicPartitions().stream()
                                  .anyMatch(
                                      partition ->
                                          partition.partition() == pendingPartition.join()))
                      .findFirst()
                      .map(MemberDescription::host)
                      .map(host -> URI.create(String.format("%s:%d", host, rpcPort)));
              if (optionalURI.isEmpty()) {
                discoveredURI.completeExceptionally(
                    new RuntimeException(String.format("Failed to find host for key %s", key)));
              } else {
                discoveredURI.complete(optionalURI.get());
              }
            });

    return discoveredURI;
  }

  @Override
  public void onChange(final State newState, final State oldState) {
    logger.info("Changing to {} from {}", newState, oldState);
    if (newState.isRunningOrRebalancing()) {
      started.countDown();
    }
  }
}
