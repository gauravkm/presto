/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.cache;

import com.facebook.airlift.log.Logger;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.SettableFuture;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.toIntExact;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileCache
{
    private static final long MAX_PREFETCH_SIZE = 4L << 30;
    private static final int MAX_ENTRIES = 200000;
    // Constant from MurMur hash.
    private static final long M = 0xc6a4a7935bd1e995L;
    private static final Logger log = Logger.get(FileCache.class);

    private static final List<WeakReference<FileToken>>[] fileTokens = new ArrayList[1024];
    private static final List<Entry>[] entryHashTable = new ArrayList[32 * 1024];
    private static Entry[] entries;
    private static final ReferenceQueue<byte[]> gcdBuffers = new ReferenceQueue<>();
    private static volatile int clockHand;
    private static int numEntries;
    // Anything with age > evictionThreshold is evictable.
    private static long evictionThreshold = Long.MIN_VALUE;
    // numGets at the time evictionThreshold was last computed.
    private static long numGetsForStats;
    // Place in entries where to start the next sampling.
    private static int statsClockHand;
    private static long statsTime;
    private static boolean useThreshold = true;
    // True until being at capacity for the first time.
    private static boolean initializing = true;
    private static long lastWarnTime;
    private static final FileCacheStats fileCacheStats;

    static {
        fileCacheStats = new FileCacheStats();
    }

    private static AtomicLong totalSize = new AtomicLong();
    private static AtomicLong prefetchSize = new AtomicLong();
    private static long targetSize;

    private static ExecutorService prefetchExecutor;
    private static FileCacheStats statsBean;
    private static final BufferReferenceSupplier bufferReferenceSupplier = (b, i) -> new BufferReference(b, gcdBuffers, i);

    // LRU cache from table.column to per column-wise access listener.
    private static final LoadingCache<String, Listener> listeners =
            CacheBuilder.newBuilder()
                    .maximumSize(10000)
                    .removalListener(new RemovalListener<String, Listener>()
                    {
                        public void onRemoval(RemovalNotification<String, Listener> notification)
                        {
                            defaultListener.merge(notification.getValue());
                        }
                    })
                    .build(CacheLoader.from(key -> new Listener(key)));
    private static final Listener defaultListener = new Listener("unspecified");

    private FileCache() {}

    public static void incrementTargetSize(long bytes)
    {
        long free = Runtime.getRuntime().freeMemory();
        long max = Runtime.getRuntime().maxMemory();
        long total = Runtime.getRuntime().totalMemory();
        long newSize = targetSize + bytes;
        if (newSize <= 0) {
            log.warn("Setting FileCache size to 0");
            targetSize = 0;
        }
        else if (bytes > free + (max - total) - (500 * (1014 * 1024))) {
            log.warn("Attempting to set FileCache size above free memory - 500M: Increase by " + bytes + " while " + (free + max - total) + "available. Total = " + total + " and max = " + max);
            targetSize = (max - total) / 2;
            if (targetSize <= 1L << 30) {
                targetSize += 1L << 30;
            }
        }
        else {
            targetSize += bytes;
        }
    }

    // Returns a FileToken for path. Each path has exactly one FileToken.
    public static FileToken getFileToken(String path)
    {
        int hash = path.hashCode();
        int index = hash & (fileTokens.length - 1);
        List<WeakReference<FileToken>> list = fileTokens[index];
        if (list == null) {
            synchronized (FileToken.class) {
                list = fileTokens[index];
                if (list == null) {
                    list = new ArrayList();
                    fileTokens[index] = list;
                }
            }
        }
        synchronized (list) {
            for (int i = 0; i < list.size(); i++) {
                WeakReference<FileToken> reference = list.get(i);
                FileToken token = reference.get();
                if (token == null) {
                    list.remove(i);
                    i--;
                    continue;
                }
                if (token.matches(path, hash)) {
                    return token;
                }
            }
            FileToken token = new FileToken(path);
            list.add(new WeakReference(token));
            return token;
        }
    }

    public static Listener getListener(String label)
    {
        if (label == null) {
            return defaultListener;
        }
        try {
            return listeners.get(label);
        }
        catch (Exception e) {
            return defaultListener;
        }
    }

    public static Entry get(ILoader dataSource, long offset, int size)
            throws IOException
    {
        fileCacheStats.incrementNumGets();
        return getInternal(dataSource, offset, size, false, System.nanoTime(), null, 100);
    }

    public static Entry get(ILoader dataSource, long offset, int size, Listener listener, int retentionWeight)
            throws IOException
    {
        fileCacheStats.incrementNumGets();
        return getInternal(dataSource, offset, size, false, System.nanoTime(), listener, retentionWeight);
    }

    private static Entry getInternal(ILoader dataSource, long offset, int size, boolean isPrefetch, long now, Listener listener, int retentionWeight)
            throws IOException
    {
        FileToken token = dataSource.getToken();
        long hash = hashMix(token.hashCode(), offset);
        int bucketIndex = (int) hash & (entryHashTable.length - 1);
        List<Entry> bucket = entryHashTable[bucketIndex];
        // get or create bucket
        if (bucket == null) {
            synchronized (FileCache.class) {
                ensureEntries();
                bucket = entryHashTable[bucketIndex];
                if (bucket == null) {
                    bucket = new ArrayList();
                    entryHashTable[bucketIndex] = bucket;
                }
            }
        }
        int retryCount = 0;
        retry:
        while (true) {
            trimGcd();
            SettableFuture futureToWait = null;
            Entry entryToWait = null;
            Entry entryToLoad = null;
            Entry hitEntry = null;
            synchronized (bucket) {
                /*
                Go through all the entries in the buffer,
                Check if entry is valid
                Check if entry matches token
                If entry is loading and a pre-fetch, then return null immediately.
                If entry is loading but not prefetch, then we should wait for this entry to load.

                If entry is not loading, and already available by prefetch, then update the entry access time and return null?
                Remaining case: Entry is not loading,
                it is not prefetch but enough data available  -> it is a hit entry
                It is not prefetch and not enough data available -> Then we need to load data.

                If not prefetch and not data available
                it is prefetch but no soft reference

                In both cases (that if data is not available), if entry's soft reference is null, then we need to load it.
                If data in the entry is less than required data, then we need to load as well.
                Otherwise we have a hit.

                If we cycle through all entries, and can't find a hit or load or wait -> then we create a loading entry
                If we do find a loading entry, then we just wait on it until it loads and there is a retry involved
                Once we find a hit, we return that.

                If we neither find loading, not hit, then we go ahead and load the entry to be loaded.


                 */

                /*
                if (cache.containsKey(cachekey)) {
                    // return this entry
                 } else {
                    // prepare to load entry
                 }

                 For the returned entry, we can check the state:
                 LOADING -> need to wait for data if not prefetch.
                 VALID_DATA -> return this data
                 GC'ed -> need to issue call to read this data and wait for it, if not pre-fetch.

                 */
                for (Entry entry : bucket) {
                    entry.isValid(bucketIndex);
                    if (entry.matches(token, offset)) {
                        futureToWait = entry.getLoadingFuture();
                        // If entry is loading, check whether its a pre-fetch
                        if (entry.isLoadPending()) {
                            if (isPrefetch) {
                                fileCacheStats.incrementLatePrefetch();
                                return null;
                            } else {
                                fileCacheStats.incrementConcurrentMiss();
                                entryToWait = entry;
                                break;
                            }
                        }

                        //

                        if (isPrefetch && entry.getSoftBuffer().get() != null) {
                            // The entry is in, set access time to avoid eviction but do not pin.
                            entry.setAccessTime(now);
                            return null;
                        }
                        if (!entry.getShared()) {
                            continue retry;
                        }
                        // The entry is pinned. Either 1. the entry has the data and is a hit. The data has been GC'd 3. The entry has only the head (longer read than at last use.
                        entry.setBuffer(entry.getSoftBuffer().get());
                        if (entry.getBuffer() == null) {
                            // The buffer was GC'd. This will come up in
                            // the reference queue but this will be a
                            // no-op since the bucket will not contain an
                            // entry with the equal BufferReference, thus
                            // the entry will not be removed.
                            entry.setSoftBuffer(null);
                            totalSize.addAndGet(-entry.getBufferSize());
                            fileCacheStats.incrementGcdBytes(entry.getBufferSize());
                            entryToLoad = entry;
                            entry.setLoadingFuture(SettableFuture.create());
                            entry.setLoadingThread(currentThread());
                            break;
                        }
                        if (entry.getDataSize() < size) {
                            entry.setLoadingFuture(SettableFuture.create());
                            entry.setLoadingThread(currentThread());
                            entry.setPrefetch(isPrefetch);
                            entryToLoad = entry;
                            break;
                        }

                        entry.setAccessTime(now);
                        // Enforce a range so as not to wrap around.
                        entry.setAccessCount(Math.min(entry.getAccessCount() + 1, 10000));
                        if (!entry.isPrefetch()) {
                            fileCacheStats.incrementHitBytes(entry.getDataSize());
                        }
                        hitEntry = entry;
                        break;
                    }
                }
                if (entryToLoad == null && futureToWait == null && hitEntry == null) {
                    // There was a miss. While synchronized on the bucket, add an entry in loading state.
                    entryToLoad = Entry.createTemporary(token, offset, size, isPrefetch, now, bucketIndex, bufferReferenceSupplier);
                    entryToLoad.setListener(listener);
                    entryToLoad.setRetentionWeight(retentionWeight);
                    bucket.add(entryToLoad);
                }
            }
            // Not synchronized on the bucket. If somebody else already loading, wait.
            if (futureToWait != null) {
                try {
                    long startWait = System.nanoTime();
                    futureToWait.get(20000, MILLISECONDS);
                    now = System.nanoTime();
                    fileCacheStats.incrementReadWaitMicros((now - startWait) / 1000);
                }
                catch (TimeoutException e) {
                    log.warn("FileCache: Exceeded 20s waiting for other thread to load " + entryToWait.toString());
                    retryCount++;
                    if (retryCount > 2) {
                        // The entry stays in loading state. The
                        // loading thread is presumed to have errored
                        // out without removing the entry from the
                        // cache. This is an inconsistency that we
                        // cleanup here, otherwise the whole process
                        // will hang every time hitting the unfinished
                        // entry.
                        log.warn("FileCache: Entry in loading state for over 1 minute. Removing the entry from its bucket" + entryToWait.toString());
                        removeFromBucket(entryToWait, bucketIndex, true, null, false);
                    }
                    continue;
                }
                catch (Exception e) {
                    throw new IOException("Error in read signalled on other thread" + e.toString());
                }
                // The future was completed, the entry should be in.
                continue;
            }
            if (hitEntry != null) {
                boolean wasPrefetch = hitEntry.isPrefetch();
                hitEntry.setPrefetch(false);
                if (!wasPrefetch && hitEntry.getListener() != null) {
                    hitEntry.getListener().hit(hitEntry);
                }
                try {
                    verify(hitEntry.getPinCount() > 0);
                    verify(hitEntry.getBuffer() != null);
                    verify(hitEntry.getBuffer().length >= size);
                }
                catch (Exception e) {
                    removeAndThrow(hitEntry, e, bucketIndex);
                }
                return hitEntry;
            }
            Entry result = load(entryToLoad, size, dataSource, isPrefetch, bucketIndex, now);
            if (isPrefetch) {
                verify(result == null);
            }
            else {
                try {
                    verify(!result.isLoadPending() && (result.isTemporary() || result.getPinCount() > 0));
                    verify(result.getBuffer().length >= size);
                }
                catch (Exception e) {
                    removeAndThrow(result, e, bucketIndex);
                }
                return result;
            }
        }
    }

    public static void removeAndThrow(Entry hitEntry, Exception e, int bucketIndex)
            throws IOException
    {
        log.warn("FileCache: Removing bad entry " + hitEntry.toString() + " caused by " + e.toString());
        removeFromBucket(hitEntry, bucketIndex, true, null, true);
        hitEntry.loadDone(false);
        throw new IOException(e);
    }

    private static void ensureEntries()
    {
        if (entries == null) {
            entries = new Entry[Math.max(MAX_ENTRIES, toIntExact(targetSize / 200000))];
            numEntries = Math.min(200, entries.length);
            for (int i = 0; i < numEntries; i++) {
                entries[i] = new Entry(0, bufferReferenceSupplier);
            }
        }
    }

    private static long hashMix(long h1, long h2)
    {
        return h1 ^ (h2 * M);
    }

    private static void trimGcd()
    {
        while (true) {
            Reference<? extends byte[]> reference = gcdBuffers.poll();
            if (reference == null) {
                return;
            }
            BufferReference bufferReference = (BufferReference) reference;
            List<Entry> bucket = entryHashTable[bufferReference.getBucket()];
            synchronized (bucket) {
                for (int i = 0; i < bucket.size(); i++) {
                    Entry entry = bucket.get(i);
                    if (entry.getSoftBuffer() == bufferReference) {
                        totalSize.addAndGet(-entry.getBufferSize());
                        fileCacheStats.incrementGcdBytes(entry.getBufferSize());
                        bucket.remove(i);
                        checkState(entry.getBuffer() == null);
                        entry.setSoftBuffer(null);
                        entry.setToken(null);
                        entry.setBucketIndex(-1);
                    }
                }
            }
        }
    }

    private static void warnEvery10s(String message)
    {
        long now = System.nanoTime();
        if (lastWarnTime == 0 || now - lastWarnTime > 10000000000L) {
            lastWarnTime = now;
            log.warn(message);
        }
    }

    private static long freeMemory()
    {
        return Runtime.getRuntime().freeMemory();
    }

    private static void updateEvictionThreshold(long now)
    {
        // Sample a few ages  and return bottom 20 percentile.
        int end = numEntries;
        int numSamples = Math.min(end / 20, 500);
        int step = end / numSamples;
        long[] samples = new long[numSamples];
        int sampleCount = 0;
        int numLoops = 0;
        while (true) {
            int startIndex = (statsClockHand++ & 0xffffff) % end;
            for (int i = 0; i < numSamples && sampleCount < numSamples; i++, startIndex = (startIndex + step >= end) ? startIndex + step - end : startIndex + step) {
                Entry entry = entries[startIndex];
                BufferReference reference = entry.getSoftBuffer();
                if (reference != null && reference.get() != null) {
                    samples[sampleCount++] = entry.age(now);
                }
            }
            if (sampleCount >= numSamples || ++numLoops > 10) {
                break;
            }
        }
        Arrays.sort(samples, 0, sampleCount);
        statsTime = now;
        evictionThreshold = sampleCount == 0 ? Long.MIN_VALUE : samples[(sampleCount / 5) * 4];
    }

    private static Entry findOrMakeEmpty()
    {
        if (numEntries >= MAX_ENTRIES) {
            return null;
        }
        Entry newEntry = new Entry(0, bufferReferenceSupplier);
        synchronized (FileCache.class) {
            if (numEntries == MAX_ENTRIES) {
                return null;
            }
            if (!newEntry.getExclusive()) {
                verify(false, "Failed to set exclusive access on new entry");
            }
            entries[numEntries] = newEntry;
            // All below numEntries must appear filled for dirty readers.
            numEntries++;
            return newEntry;
        }
    }

    private static ExecutorService getExecutor()
    {
        if (prefetchExecutor != null) {
            return prefetchExecutor;
        }
        synchronized (FileCache.class) {
            if (prefetchExecutor != null) {
                return prefetchExecutor;
            }
            prefetchExecutor = Executors.newFixedThreadPool(60);
        }
        return prefetchExecutor;
    }

    public static void asyncPrefetch(ILoader dataSource, long offset, int size, Listener listener, int retentionWeight)
    {
        if (prefetchSize.get() > MAX_PREFETCH_SIZE) {
            return;
        }
        prefetchSize.addAndGet(size);
        try {
            getExecutor().submit(() -> {
                prefetchSize.addAndGet(-size);
                String name = currentThread().getName();
                try {
                    fileCacheStats.incrementNumPrefetch();
                    fileCacheStats.incrementPrefetchBytes(size);
                    currentThread().setName("prefetch");
                    long startTime = System.nanoTime();
                    FileCache.getInternal(dataSource, offset, size, true, startTime, listener, retentionWeight);
                    fileCacheStats.incrementPrefetchMicros((System.nanoTime() - startTime) / 1000);
                }
                catch (Exception e) {
                    log.warn("FileCache: Error in prefetch " + e.toString());
                }
                finally {
                    currentThread().setName(name);
                }
            });
        }
        catch (Exception e) {
            prefetchSize.addAndGet(-size);
            throw e;
        }
    }

    public static void registerStats(FileCacheStats stats)
    {
        statsBean = stats;
    }

    public static void replaceInBucket(Entry one, Entry other, int bucketIndex)
    {
        List<Entry> bucket = entryHashTable[bucketIndex];
        other.copyEntry(one, bucketIndex);
        synchronized (other) {
            other.setExclusive(false);
            other.setPinCount(1);
        }
        synchronized (bucket) {
            for (int i = 0; i < bucket.size(); i++) {
                if (bucket.get(i) == one) {
                    bucket.set(i, other);
                    return;
                }
            }
        }
        verify(false, "Temp entry was not found in bucket");
    }

    private static Entry getPermanentEntry(Entry tempEntry, int size, int newBucket, long now)
    {
        // Finds a suitably old entry to reuse. Periodically updates
        // stats. If no entry with the size is found, removes an
        // equivalent amount of different size entries and makes a new
        // buffer of the requested size. Does a dirty read of the pin
        // counts and scores. When finding a suitable entry, obtains
        // exclusive access and removes the entry from its bucket and
        // puts it in the new bucket, initializing from the temporary
        // entry.
        //TODO(gauravmi): check if we need to process size in any way`
//        size = byteArrayPool.getStandardSize(size);
        int numLoops = 0;
        boolean allEntriesExist = numEntries >= MAX_ENTRIES;
        long bestAge = Long.MIN_VALUE;
        long bestAgeWithSize = Long.MIN_VALUE;
        Entry best = null;
        Entry bestWithSize = null;
        Entry empty = null;
        while (true) {
            int end = numEntries;
            if (fileCacheStats.getGets() - numGetsForStats > end / 8) {
                numGetsForStats = fileCacheStats.getGets();
                if (useThreshold) {
                    now = System.nanoTime();
                    updateEvictionThreshold(now);
                }
                else {
                    evictionThreshold = Long.MIN_VALUE;
                }
            }
            if (numLoops > end * 2) {
                fileCacheStats.incrementAllInUse();
                warnEvery10s("No available entry in cache");
                return null;
            }
            if (useThreshold && numLoops >= end && numLoops < end + 20) {
                now = System.nanoTime();
                updateEvictionThreshold(now);
            }
            long threshold = evictionThreshold;
            if (threshold > Long.MIN_VALUE && numLoops > 40) {
                threshold = (long) (threshold / Math.max(1.1, (numLoops / 300.0)));
            }
            numLoops += 20;
            int startIndex = (clockHand & 0xffffff) % end;
            clockHand += 20;
            boolean atCapacity = totalSize.get() + size > targetSize || freeMemory() < 200 << (1 << 20);
            if (initializing && atCapacity) {
                initializing = false;
            }
            for (int i = 0; i < 20; i++, startIndex = startIndex >= end - 1 ? 0 : startIndex + 1) {
                Entry entry = entries[startIndex];
                if (entry.getLoadingFuture() == null) {
                    BufferReference reference = entry.getSoftBuffer();
                    if (reference == null) {
                        empty = entry;
                        if (initializing || (!atCapacity && numLoops > end / 2)) {
                            break;
                        }
                        continue;
                    }
                    long age = entry.age(now);
                    if (numLoops < end && age < evictionThreshold + (now - statsTime)) {
                        continue;
                    }
                    if (entry.getBufferSize() == size && age > bestAgeWithSize) {
                        bestWithSize = entry;
                        bestAgeWithSize = age;
                        continue;
                    }
                    if (best != null && entry.getBufferSize() > best.getBufferSize() || age > bestAge) {
                        bestAge = age;
                        best = entry;
                    }
                }
            }
            // If all memory used, free the oldest that does not have the size and recycle the oldest that had the size.
            if (atCapacity || (allEntriesExist && empty == null) ||
                    (!initializing && numLoops < 100)) {
                boolean wasPrefetch = bestWithSize != null && bestWithSize.isPrefetch();
                if (bestWithSize != null && getExclusiveAndRemoveFromBucket(bestWithSize)) {
                    fileCacheStats.incrementEvictAge(bestWithSize.getAccessTime());
                    if (bestWithSize.getListener() != null) {
                        bestWithSize.getListener().evicted(bestWithSize, now, wasPrefetch);
                    }
                    checkState(bestWithSize.getPinCount() == 0);
                    bestWithSize.setPinCount(1);
                    bestWithSize.ensureBuffer(size, newBucket);
                    replaceInBucket(tempEntry, bestWithSize, newBucket);
                    return bestWithSize;
                }
                wasPrefetch = best != null && best.isPrefetch();
                if (atCapacity && numLoops > end && best != null && getExclusiveAndRemoveFromBucket(best)) {
                    // This is safe, only one thread can successfully remove.
                    fileCacheStats.incrementEvictAge(best.getAccessTime());
                    if (best.getListener() != null) {
                        best.getListener().evicted(best, now, wasPrefetch);
                    }
                    BufferReference reference = best.getSoftBuffer();
                    byte[] buffer = reference == null ? null : reference.get();
                    if (buffer == null || buffer.length != best.getBufferSize()) {
                        log.warn("FileCache: Bad decrement of totalSize: " + best.toString());
                    }
                    totalSize.addAndGet(-best.getBufferSize());
                    best.setSoftBuffer(null);
                    best.setBuffer(null);
                    best.close();
                }
            }
            else {
                if (numLoops > 30 && empty == null) {
                    empty = findOrMakeEmpty();
                    if (empty != null) {
                        // This is a new guaranteed unused empty.
                        empty.ensureBuffer(size, newBucket);
                        replaceInBucket(tempEntry, empty, newBucket);
                        return empty;
                    }
                }
                if (empty != null) {
                    if (!empty.getExclusive()) {
                        continue;
                    }
                    if (empty.getSoftBuffer() != null) {
                        // This stopped being empty.
                        empty.close();
                        continue;
                    }
                    empty.ensureBuffer(size, newBucket);
                    replaceInBucket(tempEntry, empty, newBucket);
                    return empty;
                }
            }
        }
    }

    public static boolean removeFromBucket(Entry entry, int bucketIndex, boolean force, BufferReference reference, boolean mustFind)
    {
        if (!entry.isTemporary() && !entry.isExclusive() && !entry.isLoadPending()) {
            verify(false, "Need exclusive access or a pending load to remove from bucket: " + entry.toString());
        }
        List<Entry> bucket = entryHashTable[bucketIndex];
        synchronized (bucket) {
            if (!force && entry.getSoftBuffer() != reference) {
                return false;
            }
            if (!force && (entry.getPinCount() > 0 || entry.isLoadPending())) {
                return false;
            }
            for (int i = 0; i < bucket.size(); i++) {
                if (bucket.get(i) == entry) {
                    bucket.remove(i);
                    entry.setBucketIndex(-1);
                    if (entry.isPrefetch()) {
                        fileCacheStats.incrementWastedPrefetch(entry.getDataSize());
                        entry.setPrefetch(false);
                    }
                    return true;
                }
            }
            if (mustFind) {
                verify(false, "Attempting to remove an entry that is not in its bucket" + entry.toString());
            }
        }
        return false;
    }

    public static Entry load(Entry entry, int size, ILoader dataSource, boolean isPrefetch, int bucketIndex, long now)
            throws IOException
    {
        checkState(entry.isLoadPending());
        Entry tempEntry = entry;
        Entry permanentEntry = null;

        /*
        If entry is temporary, then create a permanent entry, else it is the permanent entry.

        If entry is temporary,
            if( a pinned and loaded permanent entry is already available), do nothing
            Else we need to cleanup this temporary entry from the map, and do buffer allocation on this entry

           Then fetch data into this temporary entry


        else

         */

        try {
            if (entry.isTemporary()) {
                permanentEntry = getPermanentEntry(entry, size, bucketIndex, now);
            }
            else {
                permanentEntry = entry;
            }
        }
        catch (Exception e) {
            log.warn("Error in getting permanent entry: " + e.toString());
            throw e;
        }
        try {
            if (entry.isTemporary()) {
                if (permanentEntry != null) {
                    verify(permanentEntry.isLoadPending() && permanentEntry.getPinCount() == 1);
                    entry = permanentEntry;
                }
                else {
                    // If this is not a prefetch, this must succeed. Remove the temporary entry from the hash table and give it a buffer not owned by the cache.
                    removeFromBucket(entry, bucketIndex, true, null, true);
                    if (isPrefetch) {
                        entry.setSoftBuffer(null);
                        entry.loadDone(false);
                        return null;
                    }
                    entry.setBuffer(newBuffer(size));
                }
                long startRead = 0;
                if (!isPrefetch) {
                    startRead = System.nanoTime();
                }
                try {
                    dataSource.readFully(entry.getOffset(), entry.getBuffer(), 0, size);
                }
                catch (Exception e) {
                    log.warn("load error 1" + e.toString());
                    throw (e);
                }
                if (isPrefetch) {
                    fileCacheStats.incrementPrefetchReadBytes(size);
                }
                else {

                    long operatorReadMicros = (System.nanoTime() - startRead) / 1000;
                    fileCacheStats.incrementOperatorRead(operatorReadMicros, size);
                }
            }
            else {
                // If an entry is requested with a greater size than last request, there may be references to the buffer. Make a new buffer and replace only after loading.
                BufferReference reference = entry.getSoftBuffer();
                byte[] oldBuffer = reference != null ? reference.get() : null;
                byte[] newBuffer;
                if (oldBuffer == null || oldBuffer.length < size) {
                    newBuffer = newBuffer(size);
                    totalSize.addAndGet(newBuffer.length - (oldBuffer != null ? oldBuffer.length : 0));
                }
                else {
                    newBuffer = oldBuffer;
                }
                long startRead = 0;
                if (!isPrefetch) {
                    startRead = System.nanoTime();
                }
                try {
                    dataSource.readFully(entry.getOffset(), newBuffer, 0, size);
                }
                catch (Exception e) {
                    log.warn("load error 2" + e.toString());
                    throw (e);
                }
                if (isPrefetch) {
                    fileCacheStats.incrementPrefetchReadBytes(size);
                }
                else {

                    long operatorReadMicros = (System.nanoTime() - startRead) / 1000;
                    fileCacheStats.incrementOperatorRead(operatorReadMicros, size);
                }
                entry.setBuffer(newBuffer);
                entry.setBufferSize(newBuffer.length);
                entry.setSoftBuffer(new BufferReference(newBuffer, gcdBuffers, bucketIndex));
                entry.setDataSize(size);
            }
            entry.setAccessCount(0);
            entry.setAccessTime(System.nanoTime());
            if (!entry.isTemporary() && tempEntry != null && tempEntry.isTemporary()) {
                int count = entry.getPinCount();
                if (count != 1) {
                    log.warn("FileCache: pin count after load must always be 1: " + count + " seen, " + entry.toString());
                    verify(false, "A newly acquired permanent entry has pinCount != 1: " + entry.toString());
                }
            }
            entry.loadDone(true);
            if (entry.getListener() != null) {
                entry.getListener().loaded(entry);
            }
            if (isPrefetch) {
                entry.close();
                return null;
            }
            verify(!entry.isLoadPending());
            return entry;
        }
        catch (Exception e) {
            log.warn("FileCache: Error loading " + entry.toString() + ": " + e.toString());
            if (entry.getBucketIndex() != -1) {
                removeFromBucket(entry, bucketIndex, true, null, true);
            }
            entry.close();
            entry.loadDone(false);
            throw e;
        }
    }

    private static byte[] newBuffer(int size)
    {
        byte[] buffer = new byte[size];
        fileCacheStats.incrementBytesAllocated(buffer.length);
        return buffer;
    }

    public static boolean getExclusiveAndRemoveFromBucket(Entry entry)
    {
        if (!entry.getExclusive()) {
            return false;
        }
        BufferReference reference = entry.getSoftBuffer();
        if (reference == null || entry.getBucketIndex() == -1) {
            {
                // If there is no buffer reference, the entry cannot be in a bucket.
                entry.close();
                return false;
            }
        }
        boolean result = removeFromBucket(entry, reference.getBucket(), true, reference, true);
        if (!result) {
            verify(false, "Could not remove from bucket: " + entry.toString());
        }
        return true;
    }
}
