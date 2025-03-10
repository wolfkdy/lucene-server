package com.example.storage;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class IndexAccess {
    protected final IndexWriter indexWriter;
    protected final AtomicReference<DirectoryReader> directoryReader;
    // TODO(deyukong): implement standard IntentionLock
    protected final ReadWriteLock rwLock;
    IndexAccess(IndexWriter writer, DirectoryReader reader) {
        indexWriter = writer;
        directoryReader = new AtomicReference<DirectoryReader>(reader);
        rwLock = new ReentrantReadWriteLock();
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

    void drop() {
        rwLock.writeLock().lock();
        try {

        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
