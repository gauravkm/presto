package com.facebook.presto.cache;

public interface BufferReferenceSupplier
{
    BufferReference createBufferReference(byte[] buffer, int bucketId);
}
