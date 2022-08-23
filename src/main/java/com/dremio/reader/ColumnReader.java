package com.dremio.reader;

import java.util.concurrent.Callable;

import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import com.dremio.utills.ReaderUtil;

import software.amazon.awssdk.services.s3.S3AsyncClient;

public class ColumnReader implements Callable {
    private ColumnChunkMetaData columnData;
    private byte[] columnBytes;
    private volatile int count;
    private String path;
    private String bucket;
    private S3AsyncClient client;
    private int blockStartingPosition;
    private int columnStartingPosition;
    public ColumnReader(ColumnChunkMetaData columnData, String path, String bucket, S3AsyncClient client,int blockStartingPosition,int count)
    {
        this.columnData = columnData;
        columnBytes = new byte[(int) columnData.getTotalSize()];
        this.path = path;
        this.bucket = bucket;
        this.client = client;
        this.count = count;
        this.blockStartingPosition = blockStartingPosition;
        columnStartingPosition = (int) columnData.getStartingPos();
    }
    @Override
    public Object call() throws Exception {
        int offset = (int) (columnData.getDictionaryPageOffset() != 0 ? columnData.getDictionaryPageOffset() : columnData.getFirstDataPageOffset());
        int totalLength = (int) columnData.getTotalSize();
        performRead(offset,totalLength,totalLength);
        return columnBytes;
    }
    public void performRead(int offset, int lengthToRead, int totalColSize) throws Exception {
        if(lengthToRead<=1000000)
        {
            ReaderUtil reader = new ReaderUtil(count,lengthToRead,offset,path,bucket,client);
            //paste in the bytes to the appropriate position
            byte[] temp = (byte[]) reader.call();
            System.arraycopy(temp,0,columnBytes,offset-columnStartingPosition,lengthToRead);
            return;
        }
        else
        {
            ReaderUtil reader = new ReaderUtil(count,1000000,offset,path,bucket,client);
            byte[] temp = (byte[]) reader.call();
            //pasting in the bytes to the appropriate location
            System.arraycopy(temp,0,columnBytes,offset-columnStartingPosition,1000000);
            performRead(offset+1000000,lengthToRead-1000000,totalColSize);
        }
    }
}
