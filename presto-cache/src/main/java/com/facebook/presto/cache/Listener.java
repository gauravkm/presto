package com.facebook.presto.cache;

import java.util.concurrent.atomic.AtomicLong;

public class Listener
{
    private final String label;
    private final AtomicLong numHits = new AtomicLong();
    private final AtomicLong hitSize = new AtomicLong();
    private final AtomicLong numMisses = new AtomicLong();
    private final AtomicLong missSize = new AtomicLong();
    private final AtomicLong size = new AtomicLong();

    public Listener(String label)
    {
        this.label = label;
    }

    public void loaded(Entry entry)
    {
        size.addAndGet(entry.dataSize);
        missSize.addAndGet(entry.dataSize);
        numMisses.addAndGet(1);
    }

    public void hit(Entry entry)
    {
        numHits.addAndGet(1);
        hitSize.addAndGet(entry.dataSize);
    }

    public void evicted(Entry entry, long now, boolean wasPrefetch)
    {
        size.addAndGet(-entry.dataSize);
    }

    public void merge(Listener other)
    {
        size.addAndGet(other.size.get());
        numHits.addAndGet(other.numHits.get());
        hitSize.addAndGet(other.hitSize.get());
        numMisses.addAndGet(other.numMisses.get());
        missSize.addAndGet(other.missSize.get());
    }

    // The one with the larger hitVolume comes first.
    public int compare(Listener other)
    {
        return hitSize.get() > other.hitSize.get() ? -1 : 1;
    }

    @Override
    public int hashCode()
    {
        return label.hashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        return this == other;
    }

    @Override
    public String toString()
    {
        return label + " size " + (size.get() >> 20) + "M hits " + numHits.get() + " (" + (hitSize.get() >> 20) + "M) misses " + numMisses.get() + " (" + (missSize.get() >> 20) + "M) ";
    }
}
