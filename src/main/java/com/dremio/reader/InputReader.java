package com.dremio.reader;

public class InputReader
{
    private String bucket;
    private String path;
    private String filename;

    public InputReader(String bucket, String path, String filename) {
        this.bucket = bucket;
        this.path = path;
        this.filename = filename;
    }
}
