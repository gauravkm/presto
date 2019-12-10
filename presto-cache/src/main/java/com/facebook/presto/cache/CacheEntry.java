package com.facebook.presto.cache;

public class CacheEntry
{
    private final BufferReferenceSupplier bufferReferenceSupplier;
    private final SettableFuture<BufferReference> loadingFuture;
    private FileToken token;
    private long offset;
    private BufferReference softBuffer;
    private byte[] buffer;
    private Thread loadingThread;
    private Listener listener;
    private int dataSize;
    private int bufferSize;
    private long accessTime;
    @GuardedBy("this")
    private volatile int pinCount;
    @GuardedBy("this")
    private volatile boolean isExclusive;

    private int accessCount;
    // Tracks the bucket the entry is in. Duplicate of the bucket in softBuffer, used for consistency checking.
    private int bucketIndex = -1;
    // Controls the rate at which an entry ages. 100 means it ages at the speed of wall time. 1000 means that it ages 10x slower than wall time. This allows preferential retention of high value entries.
    private int retentionWeight = 100;
    // When finding an Entry with isTemporary set, wait for the loading future and then retry the get. A temporary entry must not be returned to a caller.
    private final boolean isTemporary;
    private boolean isPrefetch;
    private byte sizeIndex;

    private Entry(boolean isTemporary, BufferReferenceSupplier supplier)
    {
        this.isTemporary = isTemporary;
        this.bufferReferenceSupplier = supplier;
        this.loadingFuture = SettableFuture.create();
    }

    public Entry(int pinCount, BufferReferenceSupplier supplier)
    {
        this(false, supplier);
        this.pinCount = pinCount;
    }

    public static Entry createTemporary(FileToken token, long offset, int dataSize, boolean isPrefetch, long now, int bucketIndex, BufferReferenceSupplier supplier)
    {
        Entry entry = new Entry(true, supplier);
        entry.token = token;
        entry.dataSize = dataSize;
        entry.offset = offset;
        entry.isPrefetch = isPrefetch;
        entry.loadingThread = currentThread();
        entry.accessTime = now;
        entry.bucketIndex = bucketIndex;
        return entry;
    }

    public boolean isExclusive()
    {
        return isExclusive;
    }

    public synchronized boolean getExclusive()
    {
        if (pinCount == 0 && !isExclusive) {
            isExclusive = true;
            return true;
        }
        return false;
    }

    public synchronized boolean getShared()
    {
        if (!isExclusive) {
            pinCount++;
            return true;
        }
        return false;
    }

    public int getPinCount()
    {
        return pinCount;
    }

    public byte[] getBuffer()
    {
        return buffer;
    }

    @Override
    public void close()
    {
        if (isTemporary) {
            return;
        }
        synchronized (this) {
            if (isExclusive) {
                verify(pinCount == 0);
                isExclusive = false;
            }
            else {
                if (pinCount <= 0) {
                    log.warn("FileCache: Negative pin count" + toString());
                    verify(false, "Negative pinCount at close: " + toString());
                }
                if (softBuffer.getBucket() != this.bucketIndex && this.bucketIndex != -1) {
                    verify(false, "Entry has different bucketIndex and softBuffer bucketIndex: at close" + toString());
                }
                verify(buffer != null);
                pinCount--;
                if (pinCount == 0) {
                    buffer = null;
                }
            }
        }
    }

    boolean matches(FileToken token, long offset)
    {
        return this.token == token && this.offset == offset;
    }

    // Age is function of access time and access count and retentionWeight.
    public long age(long now)
    {
        long age = (now - accessTime) / (accessCount + 1);
        return retentionWeight == 100 ? age : age * 100 / retentionWeight;
    }

    public void decay()
    {
        // This is called without synchronization. Do not write negative values.
        int count = accessCount;
        if (count > 1) {
            accessCount = count - 1;
        }
    }

    public void ensureBuffer(int size, int bucketIndex)
    {
        BufferReference reference = softBuffer;
        verify(isExclusive);
        verify(this.bucketIndex == -1, "Setting a softBuffer while in another bucket");
        if (reference != null) {
            buffer = reference.get();
            if (buffer != null && buffer.length >= size) {
                softBuffer = bufferReferenceSupplier.createBufferReference(buffer, bucketIndex);
                return;
            }
        }
        if (reference == null) {
            buffer = newBuffer(size);
            softBuffer = bufferReferenceSupplier.createBufferReference(buffer, bucketIndex);
        }
        else {
            buffer = softBuffer.get();
            if (buffer == null) {
                numGcdBuffers++;
                numGcdBytes += bufferSize;
                totalSize.addAndGet(-bufferSize);
                buffer = newBuffer(size);
            }
            softBuffer = bufferReferenceSupplier.createBufferReference(buffer, bucketIndex);
        }
        bufferSize = buffer.length;
        totalSize.addAndGet(bufferSize);
    }

    public void loadDone(boolean success)
    {
        // Remove the future from the entry before marking it done. otherwise a waiting thread may see the same future on its next try and keep looping until this thread sets loadingFuture to null. Idempotent.
        if (success) {
            verify(buffer != null);
            if (!isTemporary && softBuffer == null) {
                verify(false, "Loaded permanent entry has no softBuffer: " + toString());
            }
        }
        else if (bucketIndex != -1) {
            verify(false, "Failed load leaves entry in bucket: " + toString());
        }
        loadingFuture.set(softBuffer);
    }

    @Override
    public String toString()
    {
        BufferReference reference = softBuffer;
        byte[] referencedBuffer = reference != null ? reference.get() : null;
        int referenceBucket = reference != null ? reference.getBucket() : -1;
        String bucketString = "not in bucket";
        if (referenceBucket != bucketIndex) {
            bucketString = "softBuffer bucket = " + referenceBucket + " and bucketIndex = " + bucketIndex;
        }
        else {
            bucketString = "bucket = " + bucketIndex;
        }
        return toStringHelper(this)
                .addValue(isTemporary ? "Temporary" : "")
                .addValue(token)
                .add("offset", offset)
                .add("size", dataSize)
                .add("pins ", pinCount)
                .add("buffer", referencedBuffer != null ? "byte[" + referencedBuffer.length + "]" : "null")
                .addValue(bucketString)
                .toString();
    }

    public int getDataSize()
    {
        return dataSize;
    }

    public boolean isPrefetch()
    {
        return isPrefetch;
    }

    public void setPrefetch(boolean prefetch)
    {
        this.isPrefetch = prefetch;
    }

    public boolean isTemporary()
    {
        return isTemporary;
    }

    public ListenableFuture<BufferReference> getLoadingFuture()
    {
        return loadingFuture;
    }

    public BufferReference getSoftBuffer()
    {
        return softBuffer;
    }

    public void setBuffer(byte[] bytes)
    {
        this.buffer = bytes;
    }

    public void setSoftBuffer(BufferReference bufferReference)
    {
        this.softBuffer = bufferReference;
    }

    public int getBufferSize()
    {
        return bufferSize;
    }

    public void setLoadingFuture(SettableFuture<byte[]> loadingFuture)
    {
        this.loadingFuture = loadingFuture;
    }

    public int getBucketIndex()
    {
        return bucketIndex;
    }

    public void setAccessTime(long now)
    {
        this.accessTime = now;
    }

    public void setLoadingThread(Thread currentThread)
    {
        this.loadingThread = currentThread;
    }

    public void copyEntry(Entry one, int bucketIndex)
    {
        this.token = one.token;
        this.offset = one.offset;
        this.dataSize = one.dataSize;
        checkState(this.isExclusive);
        checkState(bucketIndex == one.bucketIndex);
        checkState(one.isTemporary);
        checkState(!this.isTemporary);
        checkState(loadingFuture != null);
        this.loadingFuture = one.loadingFuture;
        this.loadingThread = one.loadingThread;
        this.isPrefetch = one.isPrefetch;
        this.listener = one.listener;
        this.retentionWeight = one.retentionWeight;
        if (this.softBuffer.getBucket() != one.bucketIndex) {
            verify(false, "Inserting entry with bad bucket index " + this.softBuffer.getBucket() + " into " + one.bucketIndex + " " + this.toString());
        }
        this.bucketIndex = one.bucketIndex;
    }

    public void setExclusive(boolean exclusive)
    {
        this.isExclusive = exclusive;
    }

    public void setPinCount(int count)
    {
        this.pinCount = count;
    }

    public void setBucketIndex(int bucketIndex)
    {
        this.bucketIndex = bucketIndex;
    }

    public long getOffset()
    {
        return offset;
    }

    public Listener getListener()
    {
        return listener;
    }

    public void setAccessCount(int accessCount)
    {
        this.accessCount = accessCount;
    }

    public long getAccessTime()
    {
        return accessTime;
    }

    public void setBufferSize(int length)
    {
        this.bufferSize = length;
    }

    public void setDataSize(int size)
    {
        this.dataSize = size;
    }

    public int getAccessCount()
    {
        return accessCount;
    }

    public void setListener(Listener listener)
    {
        this.listener = listener;
    }

    public void setRetentionWeight(int retentionWeight)
    {
        this.retentionWeight = retentionWeight;
    }

    public void setToken(Object o)
    {
        this.token = token;
    }

    public boolean isLoadPending()
    {
        return this.loadingFuture != null;
    }

    public void isValid(int bucketIndex)
    {
        if (softBuffer == null && loadingFuture == null && !isExclusive()) {
            verify(false, "Entry with no buffer and no loading future in bucket " + this.bucketIndex + ": " + toString());
        }
        else if (getSoftBuffer() != null && getSoftBuffer().getBucket() != this.bucketIndex) {
            verify(false, "Entry has bucket " + getSoftBuffer().getBucket() + " while in bucket " + bucketIndex + " " + toString());
        }
        if (getBucketIndex() != this.bucketIndex) {
            verify(false, "Entry in bucket " + bucketIndex + " entry = " + toString());
        }
    }
}
