/*
 * Copyright (C) 2017-2019 Dremio Corporation. This file is confidential and private property.
 */
package com.dremio.reader;

import static com.dremio.reader.ObjectReader.fixedLengthToRead;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

/**
 * Read footer using an AsyncByteReader.
 */
public class FooterReader {
  private static final int FOOTER_LENGTH_SIZE = 4;
  private static final int FOOTER_METADATA_SIZE = FOOTER_LENGTH_SIZE + ParquetFileWriter.MAGIC.length;
  private static final int MAGIC_LENGTH = ParquetFileWriter.MAGIC.length;
  private static final int MIN_FILE_SIZE = ParquetFileWriter.MAGIC.length + FOOTER_METADATA_SIZE;

  private static final ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();


  public static CompletableFuture<ParquetMetadata> readFooterFuture(AsyncByteReader reader, long knownFileLength, FooterRequestListener requestListener) throws IOException {
    long maxFooterLen = 16*1024*1024;
    if (knownFileLength < MIN_FILE_SIZE) {
      throw new IOException(String.format("It is not a Parquet file. File length too short. Expected at least %d bytes but only saw %d bytes.", MIN_FILE_SIZE, knownFileLength));
    }
    final int readLen = (int) Math.min(knownFileLength, fixedLengthToRead);

    requestListener.startInitialRequest();
    return reader.readFully(knownFileLength - readLen, readLen)

      .whenComplete((a, b) -> {
        requestListener.finishInitialRequest();
      })

      .thenCompose(tailBytes -> processFooter(tailBytes, reader, knownFileLength, requestListener, maxFooterLen));
  }

  private static CompletableFuture<ParquetMetadata> processFooter(final byte[] firstReadBytes,
                                                                  AsyncByteReader reader,
                                                                  long knownFileLength,
                                                                  FooterRequestListener requestListener,
                                                                  long maxFooterLen) {
    CompletableFuture<ParquetMetadata> completeFooterRead = new CompletableFuture<>();

    try {

      checkMagicBytes(firstReadBytes, firstReadBytes.length - ParquetFileWriter.MAGIC.length);
      final int size = BytesUtils.readIntLittleEndian(firstReadBytes, firstReadBytes.length - FOOTER_METADATA_SIZE);

      if (size > maxFooterLen) {
        throw new IOException("Footer size of " + " is " + size + ". Max supported footer size is " + maxFooterLen);
      }

      if (size <= firstReadBytes.length - FOOTER_METADATA_SIZE) {
        int start = firstReadBytes.length - (size + FOOTER_METADATA_SIZE);
        byte[] footerBytes = ArrayUtils.subarray(firstReadBytes, start, start + size);
        ParquetMetadata parquetMetadata = parquetMetadataConverter.readParquetMetadata(new ByteArrayInputStream(footerBytes), ParquetMetadataConverter.NO_FILTER);
        completeFooterRead.complete(parquetMetadata);
        return completeFooterRead;
      }

      // if the footer is larger than our initial read, we need to read the rest.
      int origFooterRead = firstReadBytes.length - FOOTER_METADATA_SIZE;

      requestListener.startSecondRequest();
      completeFooterRead = reader.readFully(knownFileLength - size - FOOTER_METADATA_SIZE, size - origFooterRead)
        .whenComplete((a,b) -> {
          requestListener.finishSecondRequest();
        })
        .thenApply((byte[] additionalTailBytes) -> {
          final byte[] footerBytes = new byte[size];

          System.arraycopy(additionalTailBytes, 0, footerBytes, 0, additionalTailBytes.length);
          System.arraycopy(firstReadBytes, 0, footerBytes, additionalTailBytes.length, origFooterRead);
          try {
            ParquetMetadata parquetMetadata = parquetMetadataConverter.readParquetMetadata(new ByteArrayInputStream(footerBytes), ParquetMetadataConverter.NO_FILTER);
            return parquetMetadata;
          } catch (IOException e) {
            throw new WrapException(e);
          }
        });
    } catch (IOException e) {
      completeFooterRead.completeExceptionally(new WrapException(e));
    }

    return completeFooterRead;
  }

  public static final class WrapException extends RuntimeException {
    final IOException ex;

    public WrapException(IOException ex) {
      super();
      this.ex = ex;
    }

    public IOException getIOException() {
      return ex;
    }

    public static IOException asIO(ExecutionException ex) {
      Throwable e = ex.getCause();
      if(e instanceof WrapException) {
        return ((WrapException) e).getIOException();
      } else if (e instanceof IOException){
        return (IOException) e;
      } else {
        return new IOException(e);
      }
    }
  }

  private static void checkMagicBytes(byte[] data, int offset) throws IOException {
    for (int i =0, v = offset; i < MAGIC_LENGTH; i++, v++){
      if (ParquetFileWriter.MAGIC[i] != data[v]){
        byte[] magic = ArrayUtils.subarray(data, offset, offset + MAGIC_LENGTH);
        throw new IOException(String.format("It is not a Parquet file. expected magic number %s at tail, but found %s", Arrays.toString(ParquetFileWriter.MAGIC), Arrays.toString(magic)));
      }
    }
  }

  public interface FooterRequestListener {
    default void startInitialRequest() {}
    default void finishInitialRequest() {}
    default void startSecondRequest() {}
    default void finishSecondRequest() {}
  }
}
