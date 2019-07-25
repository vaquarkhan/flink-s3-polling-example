package com.sam.pocs;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.util.Map;

public class MapDeserializer implements DeserializationSchema<Map<String, String>> {

    @Override
    public Map<String, String> deserialize(byte[] message) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(message, new TypeReference<Map<String, String>>(){});
    }

    @Override
    public boolean isEndOfStream(Map<String, String> nextElement) {
        return nextElement == null;
    }

    @Override
    public TypeInformation<Map<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Map<String, String>>(){});
    }
}