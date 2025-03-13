package com.example.storage;

import com.example.utils.Assert;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;

import java.io.IOException;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class IndexAccess {
    protected final IndexWriter indexWriter;
    protected final AtomicReference<DirectoryReader> directoryReader;
    // TODO(deyukong): implement standard IntentionLock
    protected final ReentrantReadWriteLock rwLock;
    protected final Clock clock;
    protected long lastCommitMillis;
    IndexAccess(IndexWriter writer, DirectoryReader reader, Clock c) {
        indexWriter = writer;
        directoryReader = new AtomicReference<DirectoryReader>(reader);
        rwLock = new ReentrantReadWriteLock();
        clock = c;
        lastCommitMillis = clock.millis();
    }

    IndexAccess(IndexWriter writer, DirectoryReader reader) {
        this(writer, reader, Clock.systemUTC());
    }

    protected abstract void doBatchWrite(WriteBatch batch) throws IOException;

    public final void batchWrite(WriteBatch batch) throws IOException {
        rwLock.readLock().lock();
        try {
            doBatchWrite(batch);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    protected void refreshReader() throws IOException {
        DirectoryReader oldReader = directoryReader.get();
        DirectoryReader newReader = DirectoryReader.openIfChanged(directoryReader.get());
        if (newReader != null) {
            directoryReader.compareAndExchange(oldReader, newReader);
        }
    }

    public void commitAndRefreshReader() throws IOException {
        rwLock.readLock().lock();
        try {
            commitAndRefreshReaderInLock();
        } finally {
            rwLock.readLock().unlock();
        }
    }
    protected void commitAndRefreshReaderInLock() throws IOException {
        indexWriter.commit();
        lastCommitMillis = clock.millis();
        refreshReader();
    }

    public long getLastCommitMillis() {
        return lastCommitMillis;
    }

    public long ramBytesUsed() {
        return indexWriter.ramBytesUsed();
    }

    void drop() {
        rwLock.writeLock().lock();
        try {

        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
