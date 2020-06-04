package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class OutputJsonSerializerTest {


    @Test
    public void testSimpleConversion() {
        Schema simpleSchema = SchemaBuilder.struct().field("field1", Schema.STRING_SCHEMA).field("fieldNull", Schema.OPTIONAL_STRING_SCHEMA).build();

        Struct simpleStruct = new Struct(simpleSchema);

        simpleStruct.put("field1", "test");

        OutputJsonFormatter serializer = new OutputJsonFormatter();
        byte[] data = serializer.fromConnectData("test", simpleSchema, simpleStruct);
        String dataStr = new String(data);
        System.out.println(dataStr);
//        assert dataStr == "{\"field1\":\"test\"}";
    }

}
