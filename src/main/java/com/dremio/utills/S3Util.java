package com.dremio.utills;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Builder;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.dremio.s3.S3Configs;
import com.dremio.s3.connection.SimpleAWSCredentialsProvider;

public class S3Util {

    public static Long getObjectSize(S3Configs s3Configs) {
        ObjectMetadata meta = getV1Client(s3Configs).getObjectMetadata(s3Configs.getBucket(), s3Configs.getPath());
        return meta.getContentLength();
    }

    private static AmazonS3 getV1Client(S3Configs s3Configs) {
        AmazonS3ClientBuilder builder = AmazonS3Client.builder();
        builder.withCredentials(new SimpleAWSCredentialsProvider(s3Configs));
        AwsClientBuilder.EndpointConfiguration epr
                = createEndpointConfiguration(s3Configs);
        configureEndpoint(builder, epr, s3Configs);
        final AmazonS3 client = builder.build();
        return client;
    }

    public static AwsClientBuilder.EndpointConfiguration
    createEndpointConfiguration(
            S3Configs s3Configs) {
        if (s3Configs.getEndpoint() == null || s3Configs.getEndpoint().isEmpty()) {
            return null;
        }
        return new AwsClientBuilder.EndpointConfiguration(s3Configs.getEndpoint(), s3Configs.getRegion());
    }

    private static void configureEndpoint(
            AmazonS3Builder builder,
            AmazonS3Builder.EndpointConfiguration epr, S3Configs s3Configs) {
        if (epr != null) {
            // an endpoint binding was constructed: use it.
            builder.withEndpointConfiguration(epr);
        } else {
            // no idea what the endpoint is, so tell the SDK
            // to work it out at the cost of an extra HEAD request
            builder.withForceGlobalBucketAccessEnabled(true);
            // HADOOP-17771 force set the region so the build process doesn't halt.
            String region = s3Configs.getRegion();
            if (!region.isEmpty()) {
                builder.setRegion(region);
            }
        }
    }
}
