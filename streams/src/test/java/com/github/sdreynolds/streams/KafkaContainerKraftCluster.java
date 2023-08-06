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

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.Uuid;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

public class KafkaContainerKraftCluster implements Startable {

  private final int brokersNum;

  private final Network network;

  private final Collection<KafkaContainer> brokers;

  public KafkaContainerKraftCluster(
      String confluentPlatformVersion, int brokersNum, int internalTopicsRf) {
    if (brokersNum < 0) {
      throw new IllegalArgumentException("brokersNum '" + brokersNum + "' must be greater than 0");
    }
    if (internalTopicsRf < 0 || internalTopicsRf > brokersNum) {
      throw new IllegalArgumentException(
          "internalTopicsRf '"
              + internalTopicsRf
              + "' must be less than brokersNum and greater than 0");
    }

    this.brokersNum = brokersNum;
    this.network = Network.newNetwork();

    String controllerQuorumVoters =
        IntStream.range(0, brokersNum)
            .mapToObj(brokerNum -> String.format("%d@broker-%d:9094", brokerNum, brokerNum))
            .collect(Collectors.joining(","));

    String clusterId = Uuid.randomUuid().toString();

    this.brokers =
        IntStream.range(0, brokersNum)
            .mapToObj(
                brokerNum -> {
                  return new KafkaContainer(
                          DockerImageName.parse("confluentinc/cp-kafka")
                              .withTag(confluentPlatformVersion))
                      .withNetwork(this.network)
                      .withNetworkAliases("broker-" + brokerNum)
                      .withKraft()
                      .withClusterId(clusterId)
                      .withEnv("KAFKA_BROKER_ID", brokerNum + "")
                      .withEnv("KAFKA_NODE_ID", brokerNum + "")
                      .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", controllerQuorumVoters)
                      .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", internalTopicsRf + "")
                      .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", internalTopicsRf + "")
                      .withEnv(
                          "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", internalTopicsRf + "")
                      .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", internalTopicsRf + "")
                      .withStartupTimeout(Duration.ofMinutes(1));
                })
            .collect(Collectors.toList());
  }

  public Collection<KafkaContainer> getBrokers() {
    return this.brokers;
  }

  public String getBootstrapServers() {
    return brokers.stream()
        .map(KafkaContainer::getBootstrapServers)
        .collect(Collectors.joining(","));
  }

  @Override
  public void start() {
    // Needs to start all the brokers at once
    brokers.parallelStream().forEach(GenericContainer::start);

    Unreliables.retryUntilTrue(
        30,
        TimeUnit.SECONDS,
        () -> {
          Container.ExecResult result =
              this.brokers.stream()
                  .findFirst()
                  .get()
                  .execInContainer(
                      "sh",
                      "-c",
                      "kafka-metadata-shell --snapshot /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.log ls /brokers | wc -l");
          String brokers = result.getStdout().replace("\n", "");

          return brokers != null && Integer.valueOf(brokers) == this.brokersNum;
        });
  }

  @Override
  public void stop() {
    this.brokers.stream().parallel().forEach(GenericContainer::stop);
  }
}
