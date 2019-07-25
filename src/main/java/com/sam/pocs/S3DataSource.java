package com.sam.pocs;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Map;

public class S3DataSource implements SourceFunction<String> {

    private volatile boolean isRunning = true;
    private String bucket;
    private String key;

    public S3DataSource(String bucket, String key) {
        this.bucket = bucket;
        this.key = key;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning) {
            Region region = Region.US_EAST_1;
            S3Client s3 = S3Client.builder().region(region).build();

            InputStream s3Object = s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build(),
                    ResponseTransformer.toInputStream());

            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, String> jsonMap = objectMapper.readValue(s3Object, new TypeReference<Map>(){});

            ctx.collect(jsonMap.toString());

            System.out.println(jsonMap.toString());

            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
