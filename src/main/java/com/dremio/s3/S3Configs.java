package com.dremio.s3;

public class S3Configs {

    private final String accessKey;
    private final String secretKey;
    private final String bucket;
    private final String path;

    private String region;
    private String endpoint;

    public S3Configs(String[] args) {
        if (args.length < 4) {
            throw new IllegalStateException("Please provide buckets and one object path to validate S3 Performance!");
        }
        bucket = args[1];
        path = args[2];
        region = args[3];
        if (args.length >= 6) {
            accessKey = args[4];
            secretKey = args[5];
        } else {
            accessKey = null;
            secretKey = null;
        }
        if (args.length >= 7) {
            region = args[6];
        }
        if (args.length >= 8) {
            endpoint = args[7];
        }
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getBucket() {
        return bucket;
    }

    public String getPath() {
        return path;
    }

    public String getRegion() {
        return region;
    }


    public String getEndpoint() {
        return endpoint;
    }
}
