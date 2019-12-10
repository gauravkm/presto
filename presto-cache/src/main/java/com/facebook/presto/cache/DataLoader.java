package com.facebook.presto.cache;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.Nullable;

import java.lang.ref.ReferenceQueue;
import java.util.concurrent.Executor;

public class DataLoader
        implements ILoader
{

    private static final ReferenceQueue<byte[]> gcdBuffers = new ReferenceQueue<>();

    @Override
    public Entry readFully(int readOffset, int readLength)
    {
        Entry entry = new Entry(0, (buffer, bucketId) -> new BufferReference(buffer, gcdBuffers, bucketId));

        byte[] newBuffer = new byte[readLength];
        ListenableFuture<byte[]> future = asyncDataSource.readFully(0, newBuffer, readOffset, readLength);
        Futures.addCallback(future, new FutureCallback<byte[]>()
                {
                    @Override
                    public void onSuccess(@Nullable byte[] result)
                    {
                        entry.setSoftBuffer(new BufferReference(result, gcdBuffers, 0));
                        entry.loadDone(true);
                    }

                    @Override
                    public void onFailure(Throwable t)
                    {
                        // set entry's loading future to throw an error
                    }
                },
                MoreExecutors.directExecutor());
        entry.getLoadingFuture().set(new BufferReference(newBuffer, gcdBuffers, 0));
        return entry;
    }

    @Override
    public FileToken getToken()
    {
        return null;
    }
}
