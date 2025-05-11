package com.luceneserver.commands.mongo;

import com.luceneserver.protocol.mongo.MongoMessage;
import com.luceneserver.server.mongo.MongoServer;
import com.luceneserver.storage.HnswIndexAccess;
import com.luceneserver.storage.SearchIndexAccess;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;

import java.io.IOException;

public class CmdSearch extends Command {
    private static final Logger log = LogManager.getLogger(CmdSearch.class);

    public CmdSearch() {
        super("search");
    }

    Query parseTextQuery(BsonDocument d) throws ParseException {
        final String searchAnalyzer = d.getString("searchAnalyzer").getValue();
        final Analyzer analyzer = SearchIndexAccess.newAnalyzerFromString(searchAnalyzer);
        final QueryParser parser = new QueryParser(d.getString("path").getValue(), analyzer);
        final boolean matchAll = d.getBoolean("matchAll").getValue();
        if (matchAll) {
            parser.setDefaultOperator(QueryParser.Operator.AND);
        } else {
            parser.setDefaultOperator(QueryParser.Operator.OR);
        }
        return parser.parse(d.getString("query").getValue());
    }

    Query parseCompoundQuery(BsonDocument d) throws ParseException {
        BooleanQuery mustClause = null, mustNotClause = null, shouldClause = null, filterClause = null;
        if (d.containsKey("must")) {
            BooleanQuery.Builder b = new BooleanQuery.Builder();
            for (BsonValue v : d.getArray("must").getValues()) {
                b.add(parseSingleQuery(v.asDocument()), BooleanClause.Occur.MUST);
            }
            mustClause = b.build();
        }
        if (d.containsKey("mustNot")) {
            BooleanQuery.Builder b = new BooleanQuery.Builder();
            for (BsonValue v : d.getArray("mustNot").getValues()) {
                b.add(parseSingleQuery(v.asDocument()), BooleanClause.Occur.MUST_NOT);
            }
            mustNotClause = b.build();
        }
        if (d.containsKey("should")) {
            BooleanQuery.Builder b = new BooleanQuery.Builder();
            for (BsonValue v : d.getArray("should").getValues()) {
                b.add(parseSingleQuery(v.asDocument()), BooleanClause.Occur.SHOULD);
            }
            shouldClause = b.build();
        }
        if (d.containsKey("filter")) {
            BooleanQuery.Builder b = new BooleanQuery.Builder();
            for (BsonValue v : d.getArray("filter").getValues()) {
                b.add(parseSingleQuery(v.asDocument()), BooleanClause.Occur.FILTER);
            }
            filterClause = b.build();
        }
        BooleanQuery.Builder resultBuilder = new BooleanQuery.Builder();
        if (mustClause != null) {
            resultBuilder.add(mustClause, BooleanClause.Occur.MUST);
        }
        if (mustNotClause != null) {
            resultBuilder.add(mustNotClause, BooleanClause.Occur.MUST);
        }
        if (shouldClause != null) {
            resultBuilder.add(shouldClause, BooleanClause.Occur.MUST);
        }
        if (filterClause != null) {
            resultBuilder.add(filterClause, BooleanClause.Occur.MUST);
        }
        return resultBuilder.build();
    }

    Query parseSingleQuery(BsonDocument d) throws ParseException {
        final Query query;
        if (d.containsKey("text")) {
            query = parseTextQuery(d.getDocument("text"));
        } else if (d.containsKey("compound")) {
            query = parseCompoundQuery(d.getDocument("compound"));
        } else {
            throw new ParseException("document:"  + d.toString() + " has no text or compound field");
        }
        return query;
    }

    public RawBsonDocument run(ChannelHandlerContext opCtx, MongoMessage msg) throws IOException {
        RawBsonDocument d = msg.getBody();
        String indexName = d.getString("indexName").getValue();
        final Query query;
        try {
            query = parseSingleQuery(d);
        } catch (ParseException e) {
            return Command.createErrRspWithMsg(e.getMessage());
        }
        SearchIndexAccess ia = (SearchIndexAccess) MongoServer.getInstance().getIndexCatalog().getIndex(indexName);
        if (ia == null) {
            return Command.createErrRspWithMsg("index not exists");
        }
        int limit = d.getInt32("limit").getValue();
        log.info(query.toString());
        String[] result = ia.search(query, limit);

        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
        writer.writeStartDocument();
        writer.writeInt32("ok", 1);
        writer.writeStartArray("hits");
        for (String id : result) {
            writer.writeStartDocument();
            writer.writeString("id", id);
            writer.writeEndDocument();
        }
        writer.writeEndArray();
        writer.writeEndDocument();
        writer.close();
        return new RawBsonDocument(outputBuffer.toByteArray());
    }
}