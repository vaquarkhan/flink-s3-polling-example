package com.sam.pocs;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Instant;
import java.util.List;

// Make extendable for different output types
// or maybe take in a type and have objectmapper convert to that type
public class S3DataSource<T> implements SourceFunction<T>, ResultTypeQueryable {

  private volatile boolean isRunning = true;
  // double if volatile keeps this static value threadsafe
  // how to handle multithreading?
  private volatile Instant lastModified;
  private String bucket;
  // can we have dynamic prefix?
  private String prefix;
  // add default max
  private String region;
  private DeserializationSchema<T> valueDeserializer;

  // add filter function
  // add prefix (can we have dynamic prefix?)
  // add max keys to list
  public S3DataSource(
      String bucket,
      String prefix,
      Instant lastModified,
      String region,
      DeserializationSchema<T> valueDeserializer) {
    this.bucket = bucket;
    this.prefix = prefix;
    this.lastModified = lastModified;
    this.region = region;
    this.valueDeserializer = valueDeserializer;
  }

  @Override
  public void run(SourceContext<T> ctx) throws Exception {
    while (isRunning) {
      S3Client s3 = S3Client.builder().region(Region.of(region)).build();

      List<S3Object> s3Objects =
          s3.listObjects(
                  ListObjectsRequest.builder().bucket(bucket).prefix(prefix).delimiter("/").build())
              .contents();
      Instant maxLastModified = Instant.MIN;

      for (S3Object object : s3Objects) {
        if (object.size() >= 1 && lastModified.isBefore(object.lastModified())) {
          if (maxLastModified.isBefore(object.lastModified())) {
            maxLastModified = object.lastModified();
          }
          byte[] file =
              s3.getObject(
                      GetObjectRequest.builder().bucket(bucket).key(object.key()).build(),
                      ResponseTransformer.toBytes())
                  .asByteArray();
          T result = valueDeserializer.deserialize(file);
          ctx.collect(result);
        }
      }
      if (!Instant.MIN.equals(maxLastModified)) {
        lastModified = maxLastModified;
      }

      Thread.sleep(100);
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
