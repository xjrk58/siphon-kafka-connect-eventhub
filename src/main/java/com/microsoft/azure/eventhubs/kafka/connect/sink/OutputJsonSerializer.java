package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class OutputJsonSerializer implements Serializer<JsonNode>  {

    /**
     * Serialize Jackson JsonNode tree model objects to UTF-8 JSON. Using the tree model allows handling arbitrarily
     * structured data without corresponding Java classes. This serializer also supports Connect schemas.
     */
        private final ObjectMapper objectMapper = new ObjectMapper();

        /**
         * Default constructor needed by Kafka
         */
        public OutputJsonSerializer() { }

        @Override
        public byte[] serialize(String topic, JsonNode data) {
            if (data == null)
                return null;
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Error serializing JSON message", e);
            }
        }

}
