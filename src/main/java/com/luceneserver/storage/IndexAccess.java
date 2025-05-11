package com.luceneserver.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class IndexAccess {
    private static final Logger log = LogManager.getLogger(IndexCatalog.class);

    protected final IndexWriter indexWriter;
    protected final AtomicReference<DirectoryReader> directoryReader;
    // NOTE(deyukong): rwLock is used to protect indexWriter and directoryReader
    // TODO(deyukong): implement standard IntentionLock
    protected final ReentrantReadWriteLock rwLock;
    private final Clock clock;
    private long lastCommitMillis;
    private final AtomicLong lastWriteTimestamp = new AtomicLong(0);
    private final AtomicLong lastCommittedTimestamp = new AtomicLong(0);
    private final SnapshotDeletionPolicy snapshotter = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    private IndexCommit backup = null;
    private final Path directory;
    IndexAccess(FSDirectory d, IndexWriterConfig iwc, Clock c) throws IOException {
        directory = d.getDirectory();
        iwc.setIndexDeletionPolicy(snapshotter);
        indexWriter = new IndexWriter(d, iwc);
        DirectoryReader reader = DirectoryReader.open(indexWriter);
        directoryReader = new AtomicReference<DirectoryReader>(reader);
        rwLock = new ReentrantReadWriteLock();
        clock = c;
        lastCommitMillis = clock.millis();
    }

    IndexAccess(FSDirectory d, IndexWriterConfig iwc) throws IOException {
        this(d, iwc, Clock.systemUTC());
    }

    protected  void doBatchWrite(WriteBatch batch) throws IOException {
        if (batch.autoCommit) {
            commitAndRefreshReaderInLock();
        }
    }

    public Path getDirectory() { return directory; }

    public Collection<String> beginBackup() throws IOException {
        rwLock.readLock().lock();
        try {
            if (!indexWriter.isOpen()) {
                throw new IOException("index not open or maybe dropped");
            }
            synchronized (this) {
                if (backup != null) {
                    throw new IOException("backup is still in progress");
                }
                backup = snapshotter.snapshot();
                return backup.getFileNames();
            }
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void endBackup() throws IOException {
        rwLock.readLock().lock();
        try {
            if (!indexWriter.isOpen()) {
                throw new IOException("index not open or maybe dropped");
            }
            synchronized (this) {
                if (backup == null) {
                    return;
                }
                snapshotter.release(backup);
                backup = null;
            }
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public final void batchWrite(WriteBatch batch) throws IOException {
        rwLock.readLock().lock();
        try {
            doBatchWrite(batch);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public long getCommittedTimestamp() {
        return lastCommittedTimestamp.get();
    }

    public void advanceWriteTimestamp(long ts) {
        long oldVal = lastWriteTimestamp.get();
        if (oldVal > ts) {
            throw new IllegalArgumentException("oldts " + String.valueOf(oldVal) + " greater than " + String.valueOf(ts));
        }
        lastWriteTimestamp.compareAndExchange(oldVal, ts);
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
        long oldVal = lastWriteTimestamp.get();
        indexWriter.commit();
        lastCommittedTimestamp.set(oldVal);
        lastCommitMillis = clock.millis();
        refreshReader();
    }

    public long getLastCommitMillis() {
        return lastCommitMillis;
    }

    public long ramBytesUsed() {
        rwLock.readLock().lock();
        try {
            if (!indexWriter.isOpen()) {
                return 0;
            }
            return indexWriter.ramBytesUsed();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    void drop() throws IOException {
        rwLock.writeLock().lock();
        try {
            synchronized (this) {
                if (backup != null) {
                    snapshotter.release(backup);
                    backup = null;
                }
            }
            indexWriter.commit();
            indexWriter.deleteAll();
            indexWriter.close();
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
