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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Test;

public final class FullTestIT {

  @Test
  void basicTest() throws Exception {
    try (final var cluster = new KafkaContainerKraftCluster("7.2.6", 3, 2)) {
      cluster.start();
      try (final var partitioner =
          new WebsocketPartitioner(
              cluster.getBootstrapServers(), "basic-test", "events", "user-id")) {

        assertThat(cluster.getBrokers()).hasSize(3);

        var subscription =
            partitioner.subscribe(
                """
            {
            "event-type": [ "User Created" ],
                    "user-id": [ "14" ]
                    }""");
        assertThat(subscription.queue()).isEmpty();

        final Properties producerProps = new Properties();
        producerProps.put("linger.ms", 1);
        producerProps.put(
            "value.serializer", "com.github.sdreynolds.streams.serde.JacksonSerializer");
        producerProps.put(
            "key.serializer", "org.apache.kafka.common.serialization.BytesSerializer");
        producerProps.put("bootstrap.servers", cluster.getBootstrapServers());
        final var producer = new KafkaProducer<Bytes, Map<String, String>>(producerProps);

        final var creationEvent =
            Map.of("user-id", "14", "event-type", "User Created", "name", "scott reynolds");
        try {
          producer.send(new ProducerRecord<Bytes, Map<String, String>>("events", creationEvent));
          producer.send(
              new ProducerRecord<Bytes, Map<String, String>>(
                  "events",
                  Map.of("user-id", "14", "event-type", "User Updated", "name", "Scott Reynolds")));
        } finally {
          // @TODO: close with duration?
          producer.close();
        }
        assertThat(subscription.queue().poll(60, TimeUnit.SECONDS))
            .containsExactlyEntriesOf(creationEvent);
        assertThat(subscription.queue().poll(60, TimeUnit.MILLISECONDS)).isNull();
      }
    }
  }

  //  @Test
  void restartTest() throws Exception {
    try (final var cluster = new KafkaContainerKraftCluster("7.2.6", 3, 2)) {
      cluster.start();
      Thread.sleep(java.time.Duration.ofSeconds(3));
      try (final var partitioner =
          new WebsocketPartitioner(
              cluster.getBootstrapServers(), "basic-test", "events", "user-id")) {}
      Thread.sleep(java.time.Duration.ofSeconds(3));
      try (final var partitioner =
          new WebsocketPartitioner(
              cluster.getBootstrapServers(), "basic-test", "events", "user-id")) {

        assertThat(cluster.getBrokers()).hasSize(3);

        var subscription =
            partitioner.subscribe(
                """
            {
            "event-type": [ "User Created" ],
                    "user-id": [ "14" ]
                    }""");
        assertThat(subscription.queue()).isEmpty();

        final Properties producerProps = new Properties();
        producerProps.put("linger.ms", 1);
        producerProps.put(
            "value.serializer", "com.github.sdreynolds.streams.serde.JacksonSerializer");
        producerProps.put(
            "key.serializer", "org.apache.kafka.common.serialization.BytesSerializer");
        producerProps.put("bootstrap.servers", cluster.getBootstrapServers());
        final var producer = new KafkaProducer<Bytes, Map<String, String>>(producerProps);

        final var creationEvent =
            Map.of("user-id", "14", "event-type", "User Created", "name", "scott reynolds");
        try {
          producer.send(new ProducerRecord<Bytes, Map<String, String>>("events", creationEvent));
          producer.send(
              new ProducerRecord<Bytes, Map<String, String>>(
                  "events",
                  Map.of("user-id", "14", "event-type", "User Updated", "name", "Scott Reynolds")));
        } finally {
          // @TODO: close with duration?
          producer.close();
        }
        assertThat(subscription.queue().poll(60, TimeUnit.SECONDS))
            .containsExactlyEntriesOf(creationEvent);
        assertThat(subscription.queue().poll(60, TimeUnit.SECONDS))
            .containsExactlyEntriesOf(creationEvent);
        assertThat(subscription.queue().poll(60, TimeUnit.MILLISECONDS)).isNull();
      }
    }
  }
}
