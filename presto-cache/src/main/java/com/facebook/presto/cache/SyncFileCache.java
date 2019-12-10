package com.facebook.presto.cache;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.ExecutionException;

public class SyncFileCache
{
    FileCache2 fileCache2;

    byte[] get(FileToken fileToken, int offset, int length, ILoader dataLoader) {
        ListenableFuture<byte[]> listenableFuture = fileCache2.get(fileToken, offset, length, dataLoader);
        try {
            return listenableFuture.get();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
