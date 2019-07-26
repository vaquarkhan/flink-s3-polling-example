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

public class LastModifiedS3DataSourceFunction<T> implements SourceFunction<T>, ResultTypeQueryable {

  private static final long DEFAULT_POLLING_INTERVAL_MS = 100L;

  private volatile boolean isRunning = true;
  // how to handle multithreading?
  private volatile Instant lastModified;
  private String bucket;
  private String prefix;
  private String region;
  private DeserializationSchema<T> valueDeserializer;
  private long pollingInterval;

  // add filter function
  public LastModifiedS3DataSourceFunction(
      String bucket,
      String prefix,
      Instant lastModified,
      String region,
      DeserializationSchema<T> valueDeserializer) {
    this(bucket, prefix, lastModified, region, valueDeserializer, DEFAULT_POLLING_INTERVAL_MS);
  }

  public LastModifiedS3DataSourceFunction(
      String bucket,
      String prefix,
      Instant lastModified,
      String region,
      DeserializationSchema<T> valueDeserializer,
      long pollingInterval) {
    this.bucket = bucket;
    this.prefix = prefix;
    this.lastModified = lastModified;
    this.region = region;
    this.valueDeserializer = valueDeserializer;
    this.pollingInterval = pollingInterval;
  }

  //TODO review considerations
  // For using LastModified
  // - s3 list objects api only returns up to 100 objects https://docs.aws.amazon.com/cli/latest/reference/s3api/list-objects.html
  // - will have to do something like this https://stackoverflow.com/a/27931839
  // - what is the performance impact of querying all keys once there are *many* objects in the bucket? is this just short term solution?
  // For moving files to processed folder once done
  // - there will be other consumers of data from the s3 bucket. We can't modify the source bucket.
  // - A potential solution for ^^ is to have a separate staging folder which the flink job will read from.
  //    Once processing is done for a given extract, the file will be deleted from the staging folder.
  //    Cons:
  //    - There will be a separate staging folder for the flink jobs
  //    - Another process will have to populate the staging folder or Finacle will have to also send files to this staging folder.
  //    Pros:
  //    - The Flink DAG will be very straightforward. All it would have to do is poll the s3 path

  @Override
  public void run(SourceContext<T> ctx) throws Exception {
    while (isRunning) {
      S3Client s3 = S3Client.builder().region(Region.of(region)).build();

      List<S3Object> s3Objects =
          s3.listObjects(
                  ListObjectsRequest.builder().bucket(bucket).prefix(prefix).build())
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
          ctx.collect(valueDeserializer.deserialize(file));
        }
      }
      if (!Instant.MIN.equals(maxLastModified)) {
        lastModified = maxLastModified;
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
