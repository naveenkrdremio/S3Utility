/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.s3;

import static com.amazonaws.services.s3.internal.Constants.REQUESTER_PAYS;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.SdkBaseException;
import com.amazonaws.services.s3.internal.Constants;
import com.dremio.reader.AsyncByteReader;
import com.dremio.utills.NamedThreadFactory;
import com.google.common.base.Stopwatch;

import io.netty.buffer.ByteBuf;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * The S3 async APIs are unstable. This is a replacement to use a wrapper around the sync APIs to
 * create an async API.
 * <p>
 * This is the workaround suggested in https://github.com/aws/aws-sdk-java-v2/issues/1122
 */
public final class S3AsyncByteReaderUsingSyncClient implements AsyncByteReader {
  private static final Logger logger = LoggerFactory.getLogger(S3AsyncByteReaderUsingSyncClient.class);
  private static final ExecutorService threadPool = Executors.newCachedThreadPool(new NamedThreadFactory("s3-read-"));
  private final S3Client s3;
  private final String bucket;
  private final String path;
  private final Instant instant;
  private final String threadName;
  private final boolean requesterPays;
  private final boolean ssecEnabled;
  private final String ssecKey;
  private final boolean shouldCheckTimestamp;

  public S3AsyncByteReaderUsingSyncClient(S3Client s3, S3Configs s3Configs) {
    this.s3 = s3;
    this.bucket = s3Configs.getBucket();
    this.path = s3Configs.getPath();
    long mtime = 0L;
    this.instant = (mtime != 0) ? Instant.ofEpochMilli(mtime) : null;
    this.threadName = Thread.currentThread().getName();
    this.requesterPays = false;
    this.ssecEnabled = false;
    this.ssecKey = null;
    this.shouldCheckTimestamp = false;
  }

  public CompletableFuture<Void> readFully(long offset, int len, ByteBuf dstBuf, int dstOffset) {
    S3SyncReadObject readRequest = new S3SyncReadObject(offset, len, dstBuf, dstOffset);
    logger.debug(String.format("[] Submitted request to queue for bucket {}, path {} for {}", threadName, bucket, path, range(offset, len)));
    return CompletableFuture.runAsync(readRequest, threadPool);
  }

  @Override
  public CompletableFuture<byte[]> readFully(long offset, int len) {
    return AsyncByteReader.super.readFully(offset, len);
  }

  @Override
  public void close() throws Exception {
    AsyncByteReader.super.close();
  }

  /**
   * Scaffolding class to allow easy retries of an operation.
   */
  static class RetryableInvoker {
    private final int maxRetries;
    RetryableInvoker(int maxRetries) {
      this.maxRetries = maxRetries;
    }

    <T> T invoke(Callable<T> operation) throws Exception {
      int retryCount = 0;
      while (true) {
        try {
          return operation.call();
        } catch (SdkBaseException | IOException | RetryableException e) {
          if (retryCount >= maxRetries) {
            throw e;
          }

          logger.warn("Retrying S3Async operation, exception was: {}", e.getLocalizedMessage());
          ++retryCount;
        }
      }
    }
  }

  class S3SyncReadObject implements Runnable {
    private final ByteBuf byteBuf;
    private final int dstOffset;
    private final long offset;
    private final int len;
    private final RetryableInvoker invoker;

    S3SyncReadObject(long offset, int len, ByteBuf byteBuf, int dstOffset) {
      this.offset = offset;
      this.len = len;
      this.byteBuf = byteBuf;
      this.dstOffset = dstOffset;

      // Imitate the S3AFileSystem retry logic. See
      // https://github.com/apache/hadoop/blob/trunk/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/Invoker.java
      // which is created with
      // https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/retry/RetryPolicies.java#L63
      this.invoker = new RetryableInvoker(1);
    }

    @Override
    public void run() {
      // S3 Async reader depends on S3 libraries available from application class loader context
      // Thread that runs this runnable might be created from Hive readers from a different
      // class loader context. So, always changing the context to application class loader.
      try {
        final GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
          .bucket(bucket)
          .key(path)
          .range(range(offset, len));
        if (instant != null && shouldCheckTimestamp) {
          requestBuilder.ifUnmodifiedSince(instant);
        }
        if (requesterPays) {
          requestBuilder.requestPayer(REQUESTER_PAYS);
        }
        if (ssecEnabled) {
          requestBuilder.sseCustomerAlgorithm("AES256");
          requestBuilder.sseCustomerKey(ssecKey);
        }

        final GetObjectRequest request = requestBuilder.build();
        final Stopwatch watch = Stopwatch.createStarted();

        try {
          final ResponseBytes<GetObjectResponse> responseBytes = invoker.invoke(() -> s3.getObjectAsBytes(request));
          byteBuf.setBytes(dstOffset, responseBytes.asInputStream(), len);
          logger.info("[{}] Completed request for bucket {}, path {} for {}, took {} ms", threadName, bucket, path, request.range(),
            watch.elapsed(TimeUnit.MILLISECONDS));
        } catch (NoSuchKeyException ne) {
          logger.debug("[{}] Request for bucket {}, path {} failed as requested file is not present, took {} ms", threadName,
            bucket, path, watch.elapsed(TimeUnit.MILLISECONDS));
          throw new CompletionException(
            new FileNotFoundException("File not found " + path));
        } catch (S3Exception s3e) {
              switch (s3e.statusCode()) {
                case Constants.FAILED_PRECONDITION_STATUS_CODE:
                  logger.info("[{}] Request for bucket {}, path {} failed as requested version of file not present, took {} ms", threadName,
                    bucket, path, watch.elapsed(TimeUnit.MILLISECONDS));
                  throw new CompletionException(
                    new FileNotFoundException("Version of file changed " + path));
                case Constants.BUCKET_ACCESS_FORBIDDEN_STATUS_CODE:
                  logger.info("[{}] Request for bucket {}, path {} failed as access was denied, took {} ms", threadName,
                    bucket, path, watch.elapsed(TimeUnit.MILLISECONDS));
                  throw new RuntimeException(s3e.getMessage());
                default:
                  logger.error("[{}] Request for bucket {}, path {} failed with code {}. Failing read, took {} ms", threadName, bucket, path,
                    s3e.statusCode(), watch.elapsed(TimeUnit.MILLISECONDS));
                  throw new CompletionException(s3e);
              }
        } catch (Exception e) {
          logger.error("[{}] Failed request for bucket {}, path {} for {}, took {} ms", threadName, bucket, path, request.range(),
            watch.elapsed(TimeUnit.MILLISECONDS), e);
          throw new CompletionException(e);
        }
      } catch (CompletionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private String range(long start, long len) {
    // https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
    // According to spec, the bytes should be inclusive bounded, thus inclusion of -1 to end boundar.
    return String.format("bytes=%d-%d",start, start + len - 1);
  }
}
