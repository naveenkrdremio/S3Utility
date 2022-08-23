package com.dremio.s3.connection;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.dremio.s3.S3Configs;

public class SimpleAWSCredentialsProvider implements AWSCredentialsProvider {

    private final S3Configs s3Configs;

    public SimpleAWSCredentialsProvider(S3Configs s3Configs) {
        this.s3Configs = s3Configs;
    }

    @Override
    public AWSCredentials getCredentials() {
        return new BasicAWSCredentials(s3Configs.getAccessKey(), s3Configs.getSecretKey());
    }

    @Override
    public void refresh() {

    }
}
