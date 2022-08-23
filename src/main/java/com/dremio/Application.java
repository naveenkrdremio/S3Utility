package com.dremio;

import static com.dremio.utills.S3Util.getObjectSize;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.reader.ObjectReader;
import com.dremio.s3.S3AsyncByteReaderUsingSyncClient;
import com.dremio.s3.S3Configs;
import com.dremio.s3.connection.GetS3Client;
import com.google.common.base.Stopwatch;

import software.amazon.awssdk.services.s3.S3Client;

public class Application {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        S3Configs s3Configs = new S3Configs(args);
        try {
            boolean isAsync = args[0].equalsIgnoreCase("ASYNC");
            if (isAsync) {
                final Stopwatch watch = Stopwatch.createStarted();
                logger.info("Starting async reading s3 objects using S3sync client.");
                S3Client s3Client = GetS3Client.getSyncClient(s3Configs);
                S3AsyncByteReaderUsingSyncClient syncClient = new S3AsyncByteReaderUsingSyncClient(s3Client, s3Configs);
                ObjectReader objectReader = new ObjectReader(syncClient, getObjectSize(s3Configs));
                objectReader.readFooter();
                CompletableFuture<Void> future = objectReader.readObject();
                future.get();
                logger.info("Finished reading for bucket {}, path {} took {} ms", s3Configs.getBucket(), s3Configs.getPath(),
                        watch.elapsed(TimeUnit.MILLISECONDS));
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }


    }


}
