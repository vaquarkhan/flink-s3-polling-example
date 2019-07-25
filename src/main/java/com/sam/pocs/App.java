package com.sam.pocs;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Map;

public class App {
    public static void main(String[] args) throws IOException {
        Region region = Region.US_EAST_1;
        S3Client s3 = S3Client.builder().region(region).build();


        String bucket = "bucket";
        String key = "bucket";

        InputStream s3Object = s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build(),
                ResponseTransformer.toInputStream());

        ObjectMapper objectMapper = new ObjectMapper();
        String jsonMap = objectMapper.readValue(s3Object, new TypeReference<Map>(){}).toString();
        System.out.println(jsonMap);
    }
}
