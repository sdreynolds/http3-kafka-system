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
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.event.ruler.GenericMachine;
import software.amazon.event.ruler.JsonRuleCompiler;
import software.amazon.event.ruler.Patterns;

public final class WebsocketPartitioner implements AutoCloseable {
  private final Properties settings = new Properties();
  private final GenericMachine<UUID> rulesMachine = new GenericMachine<>();
  private final ConcurrentHashMap<UUID, Subscription> rules = new ConcurrentHashMap<>();
  private final KafkaStreams streamApplication;

  private static Logger logger = LoggerFactory.getLogger(WebsocketPartitioner.class);

  public WebsocketPartitioner(
      String bootstrapServers,
      final String applicationName,
      final String topicName,
      final String jsonPartitionPath) {

    // Set a few key parameters
    settings.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName);
    settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // @TODO: the test runs too fast and results in failures when this is lower than 300
    settings.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "300");

    final var builder = new StreamsBuilder();
    builder.stream(
            topicName,
            Consumed.with(Serdes.Bytes(), new JacksonSerde<>(new ObjectMapper(), Map.class)))
        .<String>groupBy(
            (key, jsonString) -> jsonString.get(jsonPartitionPath).toString(),
            Grouped.with(Serdes.String(), new JacksonSerde<>(new ObjectMapper(), Map.class)))
        .windowedBy(
            SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(30), Duration.ofSeconds(1)))
        .<Inbox>aggregate(
            () -> new Inbox(new ArrayList<>(), new HashMap<>()),
            (partitionKey, newEvent, inbox) -> {
              final var events = new ArrayList<>(inbox.items());
              final var cursors = new HashMap<>(inbox.cursorPosition());

              events.add(newEvent);
              for (int eventIdx = 0; eventIdx < events.size(); eventIdx++) {
                matchAndSendEvent(eventIdx, newEvent, cursors);
              }

              return new Inbox(events, cursors);
            },
            Materialized.as("partioned-inbox")
                .<String, Inbox, WindowStore<Bytes, byte[]>>with(
                    Serdes.String(), new JacksonSerde<Inbox>(new ObjectMapper(), Inbox.class)));

    streamApplication = new KafkaStreams(builder.build(), settings);
    streamApplication.setStateListener(null);
    streamApplication.start();
  }

  private void matchAndSendEvent(
      final Integer eventIdx, final Map<String, Object> event, final Map<UUID, Integer> cursors) {
    final JsonNode jacksonEvent = new ObjectMapper().convertValue(event, JsonNode.class);
    logger.info("Processing the following event: {}", jacksonEvent);
    final var matchingSubIds = rulesMachine.rulesForJSONEvent(jacksonEvent);
    matchingSubIds.forEach(
        subId -> {
          final var subscription = rules.get(subId);
          if (subscription != null) {
            if (cursors.getOrDefault(subId, -1) < eventIdx) {
              cursors.put(subId, eventIdx);
              subscription.queue().offer(event);
            }
          }
        });
  }

  @Override
  public void close() {
    streamApplication.close(Duration.ofMinutes(2));
    rules.forEachValue(0, sub -> unsubscribe(sub));
  }

  public Subscription subscribe(final String rule) throws IOException {
    var compiledPatterns = JsonRuleCompiler.compile(rule);
    return subscribe(compiledPatterns);
  }

  public Subscription subscribe(final List<Map<String, List<Patterns>>> compiledPatterns) {
    final var ruleId = UUID.randomUUID();
    final LinkedBlockingQueue<Map<String, Object>> queue = new LinkedBlockingQueue<>();
    final var sub = new Subscription(ruleId, queue, compiledPatterns);
    this.rules.put(ruleId, sub);
    compiledPatterns.forEach(r -> rulesMachine.addPatternRule(ruleId, r));
    return sub;
  }

  public void unsubscribe(final Subscription sub) {
    sub.rules().forEach(r -> rulesMachine.deletePatternRule(sub.id(), r));
    // @TODO: assert sub.queue() == rules.remove(sub.id())
    rules.remove(sub.id());
    sub.queue().clear();
  }
}

record Subscription(
    UUID id, BlockingQueue<Map<String, Object>> queue, List<Map<String, List<Patterns>>> rules) {}

record Inbox(List<Map<String, Object>> items, Map<UUID, Integer> cursorPosition) {}
