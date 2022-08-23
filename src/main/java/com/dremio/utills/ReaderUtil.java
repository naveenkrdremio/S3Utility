package com.dremio.utills;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * This class is used across the project whenever a query to the AWS API has to be made to read a part of an S3 object.
 * It takes the client and object details as constructor parameters and uses them to make a call via the AWS API.
 */
public class ReaderUtil implements Callable {

    private final String bucket;
    private final String path;
    private final int offset;
    private final int len;
    private  byte arr[];
    private final int ID;
    private S3AsyncClient client;

    public ReaderUtil(int ID, int len, int offset, String path, String bucket, S3AsyncClient client) {
        this.arr = new byte[len];
        this.path = path;
        this.offset = offset;
        this.len = len;
        this.bucket =  bucket;
        this.ID = ID;
        this.client  = client;
    }
    @Override
    public Object call() throws Exception {

        Instant t1 = Instant.now();
        Region region = Region.US_WEST_2;

        final GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
                .bucket(bucket)
                .key(path)
                .range(range(offset, len))
                ;

        CompletableFuture<ResponseBytes<GetObjectResponse>> response = client.getObject(requestBuilder.build(), AsyncResponseTransformer.toBytes());

        try {
            ResponseBytes<GetObjectResponse> data = response.get();
            arr = data.asByteArray();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        response.join();

        Instant t2 = Instant.now();
        Duration d = Duration.between(t1,t2);

        long timeInMilli = d.toMillis();
        //metrics.updateMetrics(timeInMilli);

        System.out.println("Completed the read to the column number  - " + ID + " with thread ID  - " + Thread.currentThread().getId()
                + "The time taken for this read was  - " + d.toMillis());
        return arr;
    }

    protected static String range(long start, long len) {
        // https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
        // According to spec, the bytes should be inclusive bounded, thus inclusion of -1 to end boundary.
        return String.format("bytes=%d-%d",start, start + len - 1);
    }
}


