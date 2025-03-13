package com.example.storage;

import com.example.server.MongoServer;
import com.example.utils.ServerParameter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.ConcurrentMergeScheduler;

import javax.management.openmbean.KeyAlreadyExistsException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Clock;
import java.util.*;

public class IndexCatalog extends Thread {
    private static final Logger log = LogManager.getLogger(IndexCatalog.class);

    public final static String META_FILE = "index_catalog.json";

    private static final ServerParameter<Integer> maxMergeThreads =
            new ServerParameter<>("maxMergeThreads", 4, true);
    private static final ServerParameter<Integer> maxMergeTasks =
            new ServerParameter<>("maxMergeTasks", 8, true);
    // hnswMaxSegmentSizeMB and invertedIndexMaxSegmentSizeMB can not be changed at runtime because It's not sure whether MergePolicy is changeable at runtime.
    private static final ServerParameter<Long> hnswMaxSegmentSizeMB =
            new ServerParameter<>("hnswMaxSegmentSizeMB", 1024L, false);
    private static final ServerParameter<Long> invertedIndexMaxSegmentSizeMB =
            new ServerParameter<>("invertedIndexMaxSegmentSizeMB", 1024L, false);
    private static final ServerParameter<Long> maxBufferedMemoryPerIndex =
            new ServerParameter<>("maxBufferedMemoryMBPerIndex", 64L, false);
    private static final ServerParameter<Long> maxBufferedMemoryMBAllIndexes =
            new ServerParameter<>("maxBufferedMemoryMBAllIndexes", 1024L, false);
    private static final ServerParameter<Long> maxIndexInMemoryMillis =
            new ServerParameter<>("maxIndexInMemoryMillis", 60*1000L, true);
    public static class IndexConfig {
        public String name;
        public String path;
        public HnswIndexAccess.HnswConfig hnswConfig;
    }

    private final Path rootDir;
    private final Map<String, IndexAccess> indexes;
    private final ConcurrentMergeScheduler mergeScheduler;
    private final Clock clock;

    public static void touch() {}

    public void close() {
        interrupt();
        try {
            join();
        } catch (InterruptedException e) {
            log.warn("caught {} during IndexCatalog.close", e.getMessage());
        }
    }

    public void run() {
        while (!isInterrupted()) {
            try {
                sleep(1000);
                final Set<String> keySet = new HashSet<>();
                synchronized (this) {
                    long memoryUsed = 0;
                    for (Map.Entry<String, IndexAccess> entry : indexes.entrySet()) {
                        memoryUsed += entry.getValue().ramBytesUsed();
                    }
                    if (memoryUsed >= maxBufferedMemoryMBAllIndexes.get()) {
                        keySet.addAll(indexes.keySet());
                    } else {
                        for (Map.Entry<String, IndexAccess> entry : indexes.entrySet()) {
                            if (entry.getValue().getLastCommitMillis() + maxIndexInMemoryMillis.get() > clock.millis()) {
                                keySet.add(entry.getKey());
                            }
                        }
                    }
                }
                for (String indexName : keySet) {
                    IndexAccess ia = getIndex(indexName);
                    if (ia == null) {
                        continue;
                    }
                    long memoryUsedBefore = ia.ramBytesUsed();
                    try {
                        ia.commitAndRefreshReader();
                    } catch (IOException e) {
                        log.error("commit {} failed with error {}", indexName, e.getMessage());
                    }
                    log.debug("index {} got a periodical commit memory from {} to {}",
                            indexName, memoryUsedBefore, ia.ramBytesUsed());
                }
            } catch (InterruptedException e) {
                log.warn("IndexCatalog exits on receiving {}", e.getMessage());
                break;
            }
        }
    }

    public IndexCatalog(String rootDir, Clock clock) throws IOException {
        super("indexCatalog");
        this.clock = clock;
        this.rootDir = Paths.get(rootDir);
        Path metaFilePath = Paths.get(rootDir, META_FILE);
        this.indexes = new TreeMap<String, IndexAccess>();
        this.mergeScheduler = new ConcurrentMergeScheduler();
        this.mergeScheduler.setMaxMergesAndThreads(maxMergeTasks.get(), maxMergeThreads.get());
        if (metaFilePath.toFile().exists()) {
            String text = Files.readString(metaFilePath);
            IndexConfig[] indexCfgs = new Gson().fromJson(text, IndexConfig[].class);
            for (IndexConfig indexCfg : indexCfgs) {
                IndexAccess ia = null;
                if (indexCfg.hnswConfig != null) {
                    ia = HnswIndexAccess.createInstance(
                            Paths.get(this.rootDir.toString(), indexCfg.path),
                            clock,
                            indexCfg.hnswConfig,
                            mergeScheduler,
                            hnswMaxSegmentSizeMB.get(),
                            maxBufferedMemoryPerIndex.get());
                } else {
                }
                if (ia != null) {
                    indexes.put(indexCfg.name, ia);
                }
            }
        }
    }

    public synchronized IndexAccess getIndex(String name) {
        return indexes.getOrDefault(name, null);
    }

    // TODO(wolfkdy): move IO(write metafile) out of mutex, use a queue for FIFO operations, Use a cv to notify IO complete.
    public synchronized void createIndex(IndexConfig cfg) throws IOException {
        if (indexes.containsKey(cfg.name)) {
            throw new IOException(cfg.name + " already exists");
        }
        IndexAccess ia = null;
        if (cfg.hnswConfig != null) {
            ia = HnswIndexAccess.createInstance(
                    Paths.get(this.rootDir.toString(), cfg.path),
                    clock,
                    cfg.hnswConfig,
                    mergeScheduler,
                    hnswMaxSegmentSizeMB.get(),
                    maxBufferedMemoryPerIndex.get());
        } else {

        }
        if (ia == null) {
            throw new IllegalArgumentException("unknown index config type");
        }
        Path metaFilePath = Paths.get(rootDir.toString(), META_FILE);
        ArrayList<IndexConfig> indexCfgs;
        if (metaFilePath.toFile().exists()) {
            String text = Files.readString(metaFilePath);
            Type indexCfgType = new TypeToken<ArrayList<IndexConfig>>(){}.getType();
            indexCfgs = new Gson().fromJson(text, indexCfgType);
        } else {
            indexCfgs = new ArrayList<>();
        }
        indexCfgs.add(cfg);
        String newCfg = new GsonBuilder().setPrettyPrinting().create().toJson(indexCfgs);
        Path tmpMetaPath = Paths.get(rootDir.toString(), META_FILE + ".tmp");
        Files.writeString(tmpMetaPath, newCfg, StandardOpenOption.CREATE);
        Files.move(tmpMetaPath, metaFilePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        indexes.put(cfg.name, ia);
    }
}
