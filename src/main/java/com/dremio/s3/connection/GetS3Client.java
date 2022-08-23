package com.dremio.s3.connection;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dremio.s3.S3Configs;
import com.dremio.utills.ApacheHttpConnectionUtil;
import com.dremio.utills.NamedThreadFactory;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsAsyncClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.client.builder.SdkSyncClientBuilder;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;

public class GetS3Client {

    private static final ExecutorService threadPool = Executors.newCachedThreadPool(new NamedThreadFactory("s3-async-read-"));

    public static S3Client getSyncClient(S3Configs s3Configs) {
        return syncConfigClientBuilder(S3Client.builder(), s3Configs).build();
    }

    public static S3AsyncClient getAsyncClient(S3Configs s3Configs) {
        return asyncConfigClientBuilder(S3AsyncClient.builder(), s3Configs).build();
    }

    private static <T extends SdkSyncClientBuilder<T,?> & AwsClientBuilder<T,?>> T syncConfigClientBuilder(T builder, S3Configs s3Configs) {

        // Note that AWS SDKv2 client will close the credentials provider if needed when the client is closed
        builder.credentialsProvider(getAsync2Provider(s3Configs))
                .httpClientBuilder(ApacheHttpConnectionUtil.initConnectionSettings(s3Configs));
        Optional<String> endpoint = Optional.ofNullable(s3Configs.getEndpoint());

        endpoint.ifPresent(e -> {
            try {
                builder.endpointOverride(new URI(e));
            } catch (URISyntaxException use) {
                throw new RuntimeException(use.getMessage());
            }
        });
        builder.region(Region.of(s3Configs.getRegion()));
        return builder;
    }

    private static <T extends AwsAsyncClientBuilder<T,?> & S3BaseClientBuilder<T,?>> T asyncConfigClientBuilder(T builder, S3Configs s3Configs) {

        builder.asyncConfiguration(b -> b.advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, threadPool))
                .credentialsProvider(getAsync2Provider(s3Configs));
        builder.region(Region.of(s3Configs.getRegion()));
        Optional<String> endpoint = Optional.of(s3Configs.getEndpoint());
        endpoint.ifPresent(e -> {
            try {
                builder.endpointOverride(new URI(e));
            } catch (URISyntaxException use) {
                throw new RuntimeException(use.getMessage());
            }
        });
        return builder;
    }

    private static AwsCredentialsProvider getAsync2Provider(S3Configs config) {
        return StaticCredentialsProvider.create(AwsBasicCredentials.create(
                config.getAccessKey(), config.getSecretKey()));
    }
}
