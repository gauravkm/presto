package com.facebook.presto.cache;

import com.google.common.util.concurrent.ListenableFuture;

public interface ILoader
{
    Entry readFully(int readOffset, int readLength);

    FileToken getToken();
}
