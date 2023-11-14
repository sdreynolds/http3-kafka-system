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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.testcontainers.Testcontainers;

public final class FindHostIT {

  @Test
  void findHost() throws Exception {
    Testcontainers.exposeHostPorts(4333);

    try (final var cluster = new KafkaContainerKraftCluster("7.2.6", 3, 2)) {
      cluster.start();

      Properties props = new Properties();
      props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers());
      try (final var admin = AdminClient.create(props)) {

        admin
            .createTopics(List.of(new NewTopic("events", 3, (short) 2)))
            .all()
            .get(300, TimeUnit.SECONDS);

        try (final var partitioner =
            new WebsocketPartitioner(
                cluster.getBootstrapServers(), "basic-test", "events", "account-id", 4333)) {

          // @TODO: without this we get a "UnknownTopicOrPartition" which is strange.
          // Could be replaced with querying the cluster containers directly?
          Thread.sleep(3_000);
          final var discovered =
              partitioner
                  .findHostForKey("123")
                  .toCompletableFuture()
                  .get(3_000, TimeUnit.MILLISECONDS);

          // @TODO: Docker Networking makes this hard
          assertEquals(URI.create("/172.25.0.1:4333").getPort(), discovered.getPort());
        }
      }
    }
  }
}
