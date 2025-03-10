package com.example.storage;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

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
    Map<String, IndexAccess> indexes;
    public static class IndexConfig {
        public String name;
        public String path;
        public HnswIndexAccess.HnswConfig hnswConfig;
    }

    public IndexCatalog(String rootDir) throws IOException {
        this.rootDir = Paths.get(rootDir);
        Path metaFilePath = Paths.get(rootDir, META_FILE);
        this.indexes = new TreeMap<String, IndexAccess>();
        if (metaFilePath.toFile().exists()) {
            String text = Files.readString(metaFilePath);
            IndexConfig[] indexCfgs = new Gson().fromJson(text, IndexConfig[].class);
            for (IndexConfig indexCfg : indexCfgs) {
                IndexAccess ia = null;
                if (indexCfg.hnswConfig != null) {
                    ia = HnswIndexAccess.createInstance(Paths.get(this.rootDir.toString(), indexCfg.path), indexCfg.hnswConfig);
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
    public synchronized void createIndex(IndexConfig cfg) throws IOException {
        if (indexes.containsKey(cfg.name)) {
            throw new IOException(cfg.name + " already exists");
        }
        IndexAccess ia = null;
        if (cfg.hnswConfig != null) {
            ia = HnswIndexAccess.createInstance(Paths.get(this.rootDir.toString(), cfg.path), cfg.hnswConfig);
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
            indexCfgs = new ArrayList<IndexConfig>();
        }
        indexCfgs.add(cfg);
        String newCfg = new GsonBuilder().setPrettyPrinting().create().toJson(indexCfgs);
        Path tmpMetaPath = Paths.get(rootDir.toString(), META_FILE + ".tmp");
        Files.writeString(tmpMetaPath, newCfg, StandardOpenOption.CREATE);
        Files.move(tmpMetaPath, metaFilePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        indexes.put(cfg.name, ia);
    }
}
