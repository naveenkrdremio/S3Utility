package com.dremio.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * This class takes in details regarding the object to be read -  bucket, path, metadata and the client used for the API queries, and writes the data to a file.
 */
public class ObjectReader {

    private static final Logger logger = LoggerFactory.getLogger(ObjectReader.class);

    private final AsyncByteReader asyncByteReader;
    private final long objectSize;
    public static final int fixedLengthToRead = 1048576;

    public ObjectReader(AsyncByteReader asyncByteReader, long objectSize) {
        this.asyncByteReader = asyncByteReader;
        this.objectSize = objectSize;
    }

    /**
     * This function iterates through the blocks in the object and assigns a thread to each one.
     * The job assigned to each job is an object of the BlockReader class.
     */
    public CompletableFuture<Void> readObject() {
        try {
            long bytesToRead = objectSize;
            long start = 0;
            final ByteBuf buf = Unpooled.directBuffer(fixedLengthToRead);
            ArrayList<CompletableFuture<Void>> combinedFutureList = new ArrayList<>();
            while (bytesToRead > 0) {
                CompletableFuture<Void> future = asyncByteReader.readFully(start, fixedLengthToRead, buf, 0);
                combinedFutureList.add(future);
                start+=fixedLengthToRead;
                bytesToRead-=fixedLengthToRead;
            }
            int nScheduled = combinedFutureList.size();
            CompletableFuture<Void>  combinedFuture = CompletableFuture.allOf(combinedFutureList.toArray(new CompletableFuture[nScheduled]));
            combinedFuture.whenComplete((v, e) -> {
                int nFailures = 0, nCancellations = 0;
                for (CompletableFuture<Void> f : combinedFutureList) {
                    if (f.isCompletedExceptionally()) {
                        ++nFailures;
                        logger.error("Exception reading range bytes!");
                    }
                    if (f.isCancelled()) {
                        ++nCancellations;
                        logger.error("Cancelled reading range bytes!");
                    }
                }
            });
            return combinedFuture;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public ParquetMetadata readFooter() throws ExecutionException, InterruptedException, IOException {
        final Stopwatch watch = Stopwatch.createStarted();
        logger.info("Starting to read footer!");
        FooterReader.FooterRequestListener listener = new FooterReader.FooterRequestListener() {
            @Override
            public void startInitialRequest() {
            }
            @Override
            public void finishInitialRequest() {
            }
        };
        CompletableFuture<ParquetMetadata> parquetMetadataCompletableFuture = FooterReader.readFooterFuture(asyncByteReader, objectSize, listener);
        ParquetMetadata parquetMetadata = parquetMetadataCompletableFuture.get();
        logger.info("Finished reading footer, It took {} ms", watch.elapsed(TimeUnit.MILLISECONDS));
        return parquetMetadata;
    }


}
