package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubClientOptions;
import com.microsoft.azure.eventhubs.EventHubException;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class EventHubClientProvider {

    private static final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(4);

    private ConnectionStringBuilder connectionStringBuilder;
    private String authenticationProvider;

    private EventHubClientProvider() {}

    public EventHubClientProvider(String authenticationProvider, String connectionString) {
        this.connectionStringBuilder = new ConnectionStringBuilder(connectionString);
        this.authenticationProvider = authenticationProvider;
    }

    public EventHubClient newInstance() throws EventHubException, IOException {
        if (authenticationProvider == EventHubSinkConfig.JWT_AUTHENTICATION_PROVIDER) {
            return getEventHubClientFromRefreshToken();
        } else {
            return getEventHubClientFromConnectionString();
        }
    }

    protected EventHubClient getEventHubClientFromRefreshToken() throws IOException {
        try {
            return EventHubClient.createWithTokenProvider(
                    connectionStringBuilder.getEndpoint(),
                    connectionStringBuilder.getEventHubName(),
                    new TokenFilesystemProvider(),
                    executorService,
                    new EventHubClientOptions()
            ).get();
        } catch (Exception ex) {
            throw new IOException("Unable to create token provider", ex);
        }
    }

    protected EventHubClient getEventHubClientFromConnectionString() throws EventHubException, IOException {
        return EventHubClient.createFromConnectionStringSync(this.connectionStringBuilder.toString(), executorService);
    }

}
