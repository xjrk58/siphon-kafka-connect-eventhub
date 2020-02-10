package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.microsoft.azure.eventhubs.ITokenProvider;
import com.microsoft.azure.eventhubs.JsonSecurityToken;
import com.microsoft.azure.eventhubs.SecurityToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class TokenFilesystemProvider implements ITokenProvider  {

    private String baseDirectory;
    private static final Logger log = LoggerFactory.getLogger(TokenFilesystemProvider.class);

    public TokenFilesystemProvider() {
        this.baseDirectory = System.getenv("CONNECT_AUTH_TOKEN_DIR");
    }

    public TokenFilesystemProvider(String baseDirectory) {
        this.baseDirectory = baseDirectory;
    }

    @Override
    public CompletableFuture<SecurityToken> getToken(String resource, Duration timeout) {
        return CompletableFuture.supplyAsync(() -> {
            try {
              log.info("Loading token for resource={} from {}", resource, baseDirectory);
              return new JsonSecurityToken(Files.readAllLines(getResourcePath(resource)).get(0), resource);
            } catch (ParseException|IOException ex) {
                throw  new RuntimeException(ex);
            }});
    }

    private Path getResourcePath(String resource) {
        return Paths.get(baseDirectory, resource.substring(resource.indexOf("/")));
    }

}
