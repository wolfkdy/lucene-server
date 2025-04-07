package com.luceneserver.storage;

import com.luceneserver.utils.ServerParameter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.ConcurrentMergeScheduler;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
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
    private static final ServerParameter<Integer> hnswMaxSegmentSizeMB =
            new ServerParameter<>("hnswMaxSegmentSizeMB", 1024, false);
    private static final ServerParameter<Integer> invertedIndexMaxSegmentSizeMB =
            new ServerParameter<>("invertedIndexMaxSegmentSizeMB", 1024, false);
    private static final ServerParameter<Integer> maxBufferedMemoryPerIndex =
            new ServerParameter<>("maxBufferedMemoryMBPerIndex", 64, false);
    private static final ServerParameter<Integer> maxBufferedMemoryMBAllIndexes =
            new ServerParameter<>("maxBufferedMemoryMBAllIndexes", 1024, false);
    private static final ServerParameter<Integer> maxIndexInMemoryMillis =
            new ServerParameter<>("maxIndexInMemoryMillis", 60*1000, true);
    public static class IndexConfig {
        public String name;
        public String path;
        public HnswIndexAccess.HnswConfig hnswConfig;
    }

    private final Path rootDir;
    private final Map<String, IndexAccess> indexes;
    private final ConcurrentMergeScheduler mergeScheduler;
    private final Clock clock;

    public static void loadClass() {}

    public void close() {
        interrupt();
        try {
            join();
        } catch (InterruptedException e) {
            log.warn("caught {} during IndexCatalog.close", e.getMessage());
        }
    }

    public synchronized void advanceWriteTimestamp(long ts) {
        for (Map.Entry<String, IndexAccess> entry : indexes.entrySet()) {
            try {
                entry.getValue().advanceWriteTimestamp(ts);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("index " + entry.getKey() + " " + e.getMessage());
            }
        }
    }

    public synchronized long getCommittedTimestamp() {
        long result = Long.MAX_VALUE;
        for (Map.Entry<String, IndexAccess> entry : indexes.entrySet()) {
            result = Long.min(result, entry.getValue().getCommittedTimestamp());
        }
        return result;
    }

    public synchronized Set<String> getAllIndexNames() {
        return indexes.keySet();
    }

    public Collection<String> beginBackup() throws IOException {
        final Set<Map.Entry<String, IndexAccess>> entries;
        synchronized (this) {
            entries = indexes.entrySet();
        }
        ArrayList<String> result = new ArrayList<>();
        for (Map.Entry<String, IndexAccess> entry : entries) {
            for (String subPath : entry.getValue().beginBackup()) {
                result.add(Paths.get(entry.getValue().getDirectory().toString(), subPath).toString());
            }
        }
        return result;
    }

    public void endBackup() throws IOException {
        final Set<Map.Entry<String, IndexAccess>> entries;
        synchronized (this) {
            entries = indexes.entrySet();
        }
        for (Map.Entry<String, IndexAccess> entry : entries) {
            entry.getValue().endBackup();
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
                    if (memoryUsed >= maxBufferedMemoryMBAllIndexes.get()*1024*1024) {
                        keySet.addAll(indexes.keySet());
                    } else {
                        for (Map.Entry<String, IndexAccess> entry : indexes.entrySet()) {
                            IndexAccess ia = entry.getValue();
                            boolean shouldFlush = ia.getLastCommitMillis() + maxIndexInMemoryMillis.get() < clock.millis();
                            if (shouldFlush) {
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

    public ArrayList<IndexConfig> loadIndexConfigs() throws IOException {
        Path metaFilePath = Paths.get(rootDir.toString(), META_FILE);
        ArrayList<IndexConfig> indexCfgs;
        if (metaFilePath.toFile().exists()) {
            String text = Files.readString(metaFilePath);
            Type indexCfgType = new TypeToken<ArrayList<IndexConfig>>(){}.getType();
            indexCfgs = new Gson().fromJson(text, indexCfgType);
        } else {
            indexCfgs = new ArrayList<>();
        }
        return indexCfgs;
    }

    public void saveIndexConfigs(ArrayList<IndexConfig> indexCfgs) throws IOException {
        String newCfg = new GsonBuilder().setPrettyPrinting().create().toJson(indexCfgs);
        Path tmpMetaPath = Paths.get(rootDir.toString(), META_FILE + ".tmp");
        Path metaFilePath = Paths.get(rootDir.toString(), META_FILE);
        Files.writeString(tmpMetaPath, newCfg, StandardOpenOption.CREATE);
        Files.move(tmpMetaPath, metaFilePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
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

        ArrayList<IndexConfig> indexCfgs = loadIndexConfigs();
        indexCfgs.add(cfg);
        saveIndexConfigs(indexCfgs);

        indexes.put(cfg.name, ia);
    }

    // TODO(wolfkdy): move IO(write metafile) out of mutex, use a queue for FIFO operations, Use a cv to notify IO complete.
    public synchronized void dropIndex(String name) throws IOException {
        if (!indexes.containsKey(name)) {
            return;
        }
        IndexAccess ia = indexes.get(name);
        ia.drop();
        try (var dirStream = Files.walk(ia.getDirectory())) {
            dirStream.map(Path::toFile).sorted(Comparator.reverseOrder()).forEach(File::delete);
        }
        ArrayList<IndexConfig> indexCfgs = loadIndexConfigs();
        indexCfgs.removeIf(o -> o.name.equals(name));
        saveIndexConfigs(indexCfgs);
        indexes.remove(name);
    }
}
