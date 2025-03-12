package com.example.storage;

import com.example.utils.ServerParameter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.lucene.index.ConcurrentMergeScheduler;

import javax.management.openmbean.KeyAlreadyExistsException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class IndexCatalog {
    public final static String META_FILE = "index_catalog.json";
    private final Path rootDir;
    private final Map<String, IndexAccess> indexes;
    private final ConcurrentMergeScheduler mergeScheduler;
    private static final ServerParameter<Integer> maxMergeThreads = new ServerParameter<Integer>("maxMergeThreads", 4, true);
    private static final ServerParameter<Integer> maxMergeTasks = new ServerParameter<Integer>("maxMergeTasks", 8, true);
    // hnswMaxSegmentSizeMB and invertedIndexMaxSegmentSizeMB can not be changed at runtime because It's not sure whether MergePolicy is changeable at runtime.
    private static final ServerParameter<Integer> hnswMaxSegmentSizeMB = new ServerParameter<Integer>("hnswMaxSegmentSizeMB", 1024, false);
    private static final ServerParameter<Integer> invertedIndexMaxSegmentSizeMB = new ServerParameter<Integer>("invertedIndexMaxSegmentSizeMB", 1024, false);
    public static class IndexConfig {
        public String name;
        public String path;
        public HnswIndexAccess.HnswConfig hnswConfig;
    }

    public static void touch() {}
    public IndexCatalog(String rootDir) throws IOException {
        this.rootDir = Paths.get(rootDir);
        Path metaFilePath = Paths.get(rootDir, META_FILE);
        this.indexes = new TreeMap<String, IndexAccess>();
        this.mergeScheduler = new ConcurrentMergeScheduler();
        if (metaFilePath.toFile().exists()) {
            String text = Files.readString(metaFilePath);
            IndexConfig[] indexCfgs = new Gson().fromJson(text, IndexConfig[].class);
            for (IndexConfig indexCfg : indexCfgs) {
                IndexAccess ia = null;
                if (indexCfg.hnswConfig != null) {
                    ia = HnswIndexAccess.createInstance(Paths.get(this.rootDir.toString(), indexCfg.path), indexCfg.hnswConfig, mergeScheduler, hnswMaxSegmentSizeMB.get());
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
            ia = HnswIndexAccess.createInstance(Paths.get(this.rootDir.toString(), cfg.path), cfg.hnswConfig, mergeScheduler, hnswMaxSegmentSizeMB.get());
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
