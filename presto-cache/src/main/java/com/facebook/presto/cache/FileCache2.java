package com.facebook.presto.cache;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class FileCache2
{
    private final ConcurrentHashMap<CacheKey, Entry> concurrentHashMap = new ConcurrentHashMap<>();

    ListenableFuture<byte[]> get(FileToken fileToken, int offset, int length, ILoader dataLoader)
    {
        CacheKey key = createCacheKey(fileToken, offset);
        Entry entry = concurrentHashMap.computeIfAbsent(key, k -> dataLoader.readFully(offset, length));
        if (entry.getDataSize() < length) {
            // enough data not available - need to issue additional requests
            concurrentHashMap.remove(key);
            return get(fileToken, offset, length, dataLoader);
        }
        Executor executor = Executors.newSingleThreadExecutor();

        return Futures.transformAsync(
                entry.getLoadingFuture(),
                bufferReference -> {
                    byte[] bytes = bufferReference.get();
                    if (bytes == null) {
                        concurrentHashMap.remove(key);
                        return get(fileToken, offset, length, dataLoader);
                    }
                    else {
                        return Futures.immediateFuture(bytes);
                    }
                },
                executor);
    }

    private CacheKey createCacheKey(FileToken fileToken, int offset)
    {
        return new CacheKey();
    }
}
