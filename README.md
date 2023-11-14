## Kafka HTTP3 Streaming
Service pulls JSON events off a Kafka topic and allows HTTP3 clients to subscribe to events based on a partitioning key and [Event Ruler](https://github.com/aws/event-ruler/) filters. The service is for rich web UIs that receive JSON events in real time. These event originate on Kafka and are distrubuted across a cluster for scalability. Clients are first subscribe by a "key" present in the source events and then the clients can provide rich Event Ruler configuration to those same events.

## Work in progress
The code is being built from the ground up. It starts with tests using [test containers](https://testcontainers.com/) to run the Kafka brokers.
