package com.facebook.presto.cache;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;

public class BufferReference
        extends SoftReference<byte[]>
{
    private final int bucketIndex;

    BufferReference(byte[] buffer, ReferenceQueue queue, int bucketIndex)
    {
        super(buffer, queue);
        this.bucketIndex = bucketIndex;
    }

    public int getBucket()
    {
        return bucketIndex;
    }
}
