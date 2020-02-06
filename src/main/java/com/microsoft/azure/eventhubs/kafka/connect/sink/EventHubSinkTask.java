package com.microsoft.azure.eventhubs.kafka.connect.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.*;
import java.util.concurrent.*;

import com.microsoft.azure.eventhubs.*;

public class EventHubSinkTask extends SinkTask {
    // List of EventHubClient objects to be used during data upload
    private BlockingQueue<EventHubClient> ehClients;
    private JsonConverter valueConvertor = new JsonConverter();
    private static final Logger log = LoggerFactory.getLogger(EventHubSinkTask.class);
    private static final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(4);

    public String version() {
        return new EventHubSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("starting EventHubSinkTask");
        EventHubSinkConfig eventHubSinkConfig;
        valueConvertor.configure(props, false);
        try {
            eventHubSinkConfig = new EventHubSinkConfig(props);
        } catch (ConfigException ex) {
            throw new ConnectException("Couldn't start EventHubSinkTask due to configuration error", ex);
        }

        String connectionString = eventHubSinkConfig.getString(EventHubSinkConfig.CONNECTION_STRING);
        log.info("connection string = {}", connectionString);
        short clientsPerTask = eventHubSinkConfig.getShort(EventHubSinkConfig.CLIENTS_PER_TASK);
        log.info("clients per task = {}", clientsPerTask);

        initializeEventHubClients(connectionString, clientsPerTask);
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        log.debug("starting to upload {} records", sinkRecords.size());
        List<CompletableFuture<Void>> resultSet = new LinkedList<>();
        for (SinkRecord record : sinkRecords) {
            EventData sendEvent = null;
            EventHubClient ehClient = null;
            try {
                sendEvent = extractEventData(record);
                // pick an event hub client to send the data asynchronously
                ehClient = ehClients.take();
                resultSet.add(sendAsync(ehClient, sendEvent));
            } catch (InterruptedException ex) {
                throw new ConnectException("EventHubSinkTask interrupted while waiting to acquire client", ex);
            }
            finally {
                if(ehClient != null) {
                    ehClients.offer(ehClient);
                }
            }
        }

        log.debug("wait for {} async uploads to finish", resultSet.size());
        waitForAllUploads(resultSet);
        log.debug("finished uploading {} records", sinkRecords.size());
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    @Override
    public void stop() {
        log.info("stopping EventHubSinkTask");
        if(ehClients != null) {
            for (EventHubClient ehClient : ehClients) {
                ehClient.close();
                log.info("closing an Event hub Client");
            }
        }
    }

    protected CompletableFuture<Void> sendAsync(EventHubClient ehClient, EventData sendEvent) {
        return ehClient.send(sendEvent);
    }

    protected EventHubClient getEventHubClientFromConnectionString(String connectionString) throws EventHubException, IOException {
        return EventHubClient.createFromConnectionStringSync(connectionString, executorService);
    }

    protected int getClientCount() {
        if(ehClients != null) {
            return ehClients.size();
        }
        else {
            return 0;
        }
    }

    private void initializeEventHubClients(String connectionString, short clientsPerTask) {
        ehClients = new LinkedBlockingQueue<EventHubClient>(clientsPerTask);
        try {
            for (short i = 0; i < clientsPerTask; i++) {
                ehClients.offer(getEventHubClientFromConnectionString(connectionString));
                log.info("Created an Event Hub Client");
            }
        } catch (EventHubException | IOException ex) {
            throw new ConnectException("Exception while creating Event Hub client", ex);
        }
    }

    private EventData extractEventData(SinkRecord record) {
        EventData eventData = null;
        if (record.value() instanceof byte[]) {
            eventData = EventData.create((byte[]) record.value());
        }
        else if (record.value() instanceof EventData) {
            eventData = (EventData) record.value();
        } else if (record.value() instanceof Struct) {
            try {
                byte[] bytes = valueConvertor.fromConnectData(record.topic(), record.valueSchema(), record.value());
                eventData = EventData.create(bytes);
            } catch (Exception ex) {
                throw new ConnectException("Unable to process record="+ record.toString(), ex);
            }
        } else {
            throw new ConnectException("Data format is unsupported for EventHubSinkType [class=" + record.value().getClass().getCanonicalName()+"]");
        }

        return eventData;
    }

    private void waitForAllUploads(List<CompletableFuture<Void>> resultSet) {
        for(CompletableFuture<Void> result : resultSet) {
            try {
                result.get();
            } catch (ExecutionException | InterruptedException ex) {
                throw new ConnectException("Exception in EventHubSinkTask while sending events", ex);
            }
        }
    }
}
