package com.facebook.presto.cache;

import org.weakref.jmx.Managed;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class FileCacheStats
{
    private long numHits;
    private long numHitBytes;
    private long numEvicts;
    private long sumEvictAge;
    private long numPrefetch;
    private long prefetchBytes;
    private long numPrefetchRead;
    private long prefetchReadBytes;
    private long numLatePrefetch;
    private long numWastedPrefetch;
    private long wastedPrefetchBytes;
    private long numConcurrentMiss;
    private long numAllInUse;
    private long prefetchMicros;
    private long operatorReadMicros;
    private long numOperatorRead;
    private long operatorReadBytes;
    private long readWaitMicros;
    private long bytesAllocated;
    private long numGcdBuffers;
    private long numGcdBytes;
    private long numGets;

    @Managed
    public long getGets()
    {
        return numGets;
    }

    @Managed
    public long getHits()
    {
        return numHits;
    }

    @Managed
    public long getHitBytes()
    {
        return numHitBytes;
    }

    @Managed
    public long getPrefetch()
    {
        return numPrefetch;
    }

    @Managed
    public long getGcdBuffers()
    {
        return numGcdBuffers;
    }

    @Managed
    public long getGcdBytes()
    {
        return numGcdBytes;
    }

    @Managed
    public long getPrefetchBytes()
    {
        return prefetchBytes;
    }

    @Managed
    public long getLatePrefetch()
    {
        return numLatePrefetch;
    }

    @Managed
    public long getWastedPrefetch()
    {
        return numWastedPrefetch;
    }

    @Managed
    public long getWastedPrefetchBytes()
    {
        return wastedPrefetchBytes;
    }

    @Managed
    public long getConcurrentMiss()
    {
        return numConcurrentMiss;
    }

    @Managed
    public long getAllInUse()
    {
        return numAllInUse;
    }

    @Managed
    public long getTotalSize()
    {
        return totalSize.get();
    }

    @Managed
    public long getNumEntries()
    {
        return numEntries;
    }

    @Managed
    public long getPinned()
    {
        int count = 0;
        int end = numEntries;
        for (int i = 0; i < end; i++) {
            if (entries[i].getPinCount() != 0) {
                count++;
            }
        }
        return count;
    }

    @Managed
    public long getLoading()
    {
        int count = 0;
        int end = numEntries;
        for (int i = 0; i < end; i++) {
            if (entries[i].loadingFuture != null) {
                count++;
            }
        }
        return count;
    }

    @Managed
    public long getAllocatedSize()
    {
        long size = 0;
        int end = numEntries;
        for (int i = 0; i < end; i++) {
            BufferReference reference = entries[i].softBuffer;
            byte[] buffer = reference == null ? null : reference.get();
            if (buffer != null) {
                size += buffer.length;
            }
        }
        return size;
    }

    @Managed
    public long getUsedSize()
    {
        long size = 0;
        int end = numEntries;
        for (int i = 0; i < end; i++) {
            BufferReference reference = entries[i].softBuffer;
            byte[] buffer = reference == null ? null : reference.get();
            if (buffer != null) {
                size += entries[i].dataSize;
            }
        }
        return size;
    }

    @Managed
    public long getPendingPrefetch()
    {
        return prefetchSize.get();
    }

    @Managed
    public long getAverageLifetimeMillis()
    {
        return sumEvictAge / 1000000 / (numEvicts | 1);
    }

    @Managed
    public long getPrefetchMicros()
    {
        return prefetchMicros;
    }

    @Managed
    public long getNumPrefetchRead()
    {
        return numPrefetchRead;
    }

    @Managed
    public long getPrefetchReadBytes()
    {
        return prefetchReadBytes;
    }

    @Managed
    public long getReadWaitMicros()
    {
        return readWaitMicros;
    }

    @Managed
    public long getNumOperatorRead()
    {
        return numOperatorRead;
    }

    @Managed
    public long getOperatorReadBytes()
    {
        return operatorReadBytes;
    }

    @Managed
    public long getOperatorReadMicros()
    {
        return operatorReadMicros;
    }

    @Managed
    public long getBytesAllocated()
    {
        return bytesAllocated;
    }

    @Managed
    public String getSizeReport()
    {
        int numSizes = byteArrayPool.getStandardSizes().length;
        long[] ages = new long[numSizes];
        long[] unhitPrefetchBytes = new long[numSizes];
        int[] counts = new int[numSizes];
        int end = numEntries;
        long now = System.nanoTime();
        for (int i = 0; i < end; i++) {
            Entry entry = entries[i];
            BufferReference reference = entry.softBuffer;
            byte[] buffer = reference == null ? null : reference.get();
            if (buffer != null) {
                int sizeIndex = byteArrayPool.getSizeIndex(buffer.length);
                ages[sizeIndex] += entry.age(now);
                counts[sizeIndex]++;
                if (entry.isPrefetch) {
                    unhitPrefetchBytes[sizeIndex] += entry.bufferSize;
                }
            }
        }
        String result = "Sizes:\n";
        for (int i = 0; i < numSizes; i++) {
            if (counts[i] > 0) {
                long size = byteArrayPool.getStandardSizes()[i];
                long percent = (size * counts[i] * 100) / totalSize.get();
                long pendingSize = unhitPrefetchBytes[i] / (1024 * 1024);
                result = result + (size / 1024) + "K: " + percent + "% " + ((size * counts[i]) >> 20) + "M Age ms: " + ages[i] / counts[i] / 1000000 + (pendingSize > 0 ? " pending use " + pendingSize + "M" : "") + "\n";
            }
        }
        return result;
    }

    @Managed
    public String getHitReport()
    {
        Set<Listener> listeners = new HashSet<>();
        int end = numEntries;
        for (int i = 0; i < end; i++) {
            Entry entry = entries[i];
            BufferReference reference = entry.softBuffer;
            byte[] buffer = reference == null ? null : reference.get();
            if (buffer != null) {
                Listener listener = entry.listener;
                if (listener != null) {
                    listeners.add(listener);
                }
            }
        }
        Listener[] array = listeners.toArray(new Listener[listeners.size()]);
        Arrays.sort(array, Listener::compare);
        StringBuilder result = new StringBuilder();
        int chars = 0;
        for (int i = 0; i < 100 && i < array.length; i++) {
            Listener listener = array[i];
            String line = listener.toString();
            result.append(line);
            result.append("\n");
            chars += line.length();
            if (chars > 20000) {
                break;
            }
        }
        return result.toString();
    }

    public void incrementNumPrefetch()
    {
        numPrefetch++;
    }

    public void incrementPrefetchBytes(int size)
    {
        prefetchBytes += size;
    }

    public void incrementPrefetchMicros(long micros)
    {
        prefetchMicros += micros;
    }

    public void incrementAllInUse()
    {
        numAllInUse++;
    }

    public void incrementNumGets()
    {
        numGets++;
    }

    public void incrementGcdBytes(int bytes)
    {
        numGcdBuffers++;
        numGcdBytes += bytes;
    }

    public void incrementBytesAllocated(int length)
    {
        bytesAllocated += length;
    }

    public void incrementReadWaitMicros(long micros)
    {
        readWaitMicros += micros;
    }

    public void incrementOperatorRead(long operatorReadMicros, int size)
    {
        numOperatorRead++;
        this.operatorReadMicros += operatorReadMicros;
        operatorReadBytes += size;
    }

    public void incrementPrefetchReadBytes(int size)
    {
        numPrefetchRead++;
        prefetchReadBytes += size;
    }

    public void incrementConcurrentMiss()
    {
        numConcurrentMiss++;
    }

    public void incrementLatePrefetch()
    {
        numLatePrefetch++;
    }

    public void incrementHitBytes(int dataSize)
    {
        numHits++;
        numHitBytes += dataSize;
    }

    public void incrementEvictAge(long accessTime)
    {
        sumEvictAge += accessTime;
        numEvicts++;
    }

    public void incrementWastedPrefetch(int dataSize)
    {
        numWastedPrefetch++;
        wastedPrefetchBytes += dataSize;
    }
}
