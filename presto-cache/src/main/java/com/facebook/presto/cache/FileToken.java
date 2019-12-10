package com.facebook.presto.cache;

public class FileToken
{
    private final String path;
    private final int hash;

    public FileToken(String path)
    {
        this.path = path;
        hash = path.hashCode();
    }

    public boolean matches(String path, long hash)
    {
        return this.hash == hash && this.path.equals(path);
    }

    @Override
    public int hashCode()
    {
        return hash;
    }

    @Override
    public boolean equals(Object other)
    {
        return this == other;
    }

    @Override
    public String toString()
    {
        return "FileToken:" + path;
    }
}
