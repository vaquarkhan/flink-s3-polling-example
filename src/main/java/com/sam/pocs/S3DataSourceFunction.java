package com.sam.pocs;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;

public class S3DataSourceFunction<T> implements SourceFunction<T>, ResultTypeQueryable {

  private static final long DEFAULT_POLLING_INTERVAL_MS = 1000L;

  private volatile boolean isRunning = true;
  // how to handle multithreading?
  private String bucket;
  private String prefix;
  private String region;
  private DeserializationSchema<T> valueDeserializer;
  private long pollingInterval;

  // add filter function
  public S3DataSourceFunction(
      String bucket,
      String prefix,
      String region,
      DeserializationSchema<T> valueDeserializer) {
    this(bucket, prefix, region, valueDeserializer, DEFAULT_POLLING_INTERVAL_MS);
  }

  public S3DataSourceFunction(
      String bucket,
      String prefix,
      String region,
      DeserializationSchema<T> valueDeserializer,
      long pollingInterval) {
    this.bucket = bucket;
    this.prefix = prefix;
    this.region = region;
    this.valueDeserializer = valueDeserializer;
    this.pollingInterval = pollingInterval;
  }

  @Override
  public void run(SourceContext<T> ctx) throws Exception {
    while (isRunning) {
      S3Client s3 = S3Client.builder().region(Region.of(region)).build();

      List<S3Object> s3Objects =
          s3.listObjects(
                  ListObjectsRequest.builder().bucket(bucket).prefix(prefix).build())
              .contents();

      for (S3Object object : s3Objects) {
        if (object.size() >= 1) {
          byte[] file =
                  s3.getObject(
                          GetObjectRequest.builder().bucket(bucket).key(object.key()).build(),
                          ResponseTransformer.toBytes())
                          .asByteArray();
          ctx.collect(valueDeserializer.deserialize(file));
          s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(object.key()).build());
        }
      }

      Thread.sleep(pollingInterval);
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }

  @Override
  public TypeInformation getProducedType() {
    return valueDeserializer.getProducedType();
  }
}
