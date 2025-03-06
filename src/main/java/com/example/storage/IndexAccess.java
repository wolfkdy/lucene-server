package com.example.storage;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;

public abstract class IndexAccess {
    protected final IndexWriter indexWriter;
    protected final IndexReader indexReader;
    IndexAccess(IndexWriter writer, IndexReader reader) {
        indexWriter = writer;
        indexReader = reader;
    }
}
