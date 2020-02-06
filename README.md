# Kafka Connect Azure Event Hub  [ ![Download](https://api.bintray.com/packages/xjrk58/org.github.xjrk58/azure-eventhub-connector/images/download.svg?version=2.3.1) ](https://bintray.com/xjrk58/org.github.xjrk58/azure-eventhub-connector/2.3.1/link)
Kafka Connect Azure Event Hub consists of the Sink Connector for streaming data from Kafka to Azure Event Hubs.
To build, run "mvn package"

# Assumptions about events

- Events with schema will be pushed to Azure Event Hubs as JSON
- Events without schema will be pushed as binary blob
