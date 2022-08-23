package com.dremio.reader;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import software.amazon.awssdk.services.s3.S3AsyncClient;


/**
 * This class implements the callable interface and is used to read an entire block
 * It takes in the details of the block and assigns a thread to read each column.
 * It combines all the data read into a single byte array which it returns.
 */
public class BlockReader implements Callable {
    private final String bucket;
    private final int ID;
    private final String path;
    private S3AsyncClient client;
    private BlockMetaData blockData;
    private byte[] blockBytes;
    private int blockSize;
    private int blockStartingPos;
    public BlockReader(String path, String bucket, int ID, S3AsyncClient client, BlockMetaData blockData) {
        this.path = path;
        this.bucket =  bucket;
        this.ID = ID;
        this.client  = client;
        this.blockData = blockData;
        blockSize = (int) blockData.getTotalByteSize();
        blockBytes = new byte[blockSize];
        blockStartingPos = (int) blockData.getStartingPos();
    }
    @Override
    public byte [] call() throws ExecutionException, InterruptedException {

        Instant t1 = Instant.now();

        //creating a cached thread pool
        ExecutorService executor = Executors.newFixedThreadPool(10);
        ArrayList<Future> futureArrayList = new ArrayList<>();
        //We create a new thread for each column

        List<ColumnChunkMetaData> cols = blockData.getColumns();
        int count  = 0;
        for(ColumnChunkMetaData col:cols)
        {
            //Create an instance of the ColumnReaderClass, which implements Callable interface and returns to a future a byte array with the bytes in that column
            ColumnReader reader = new ColumnReader(col,path,bucket,client,blockStartingPos,count++);
            futureArrayList.add(executor.submit(reader));
        }

        //retrieving the data from the futures and merging them to one global array

        int destPos = 0;
        int length = 0;
        int totalLength = 0;
        for( int i = 0;i<cols.size();i++)
        {

            byte[] temp = (byte[]) futureArrayList.get(i).get();
            destPos = (int) cols.get(i).getStartingPos();
            length = (int) cols.get(i).getTotalSize();
            totalLength+=length;
            System.arraycopy(temp,0,blockBytes,destPos-blockStartingPos,length);
        }
        System.out.println("For the block with ID - " + ID + "the final length written is " + totalLength + " the actual block size is -" + blockSize);

        Instant t2 = Instant.now();
        Duration d = Duration.between(t1,t2);

        executor.shutdown();

        System.out.println("Completed the read to block number - " + ID + " with thread ID  - " + Thread.currentThread().getId()
        + "The time taken for this read was  - " + d.toMillis());
        return blockBytes;
    }
}
