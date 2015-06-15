package de.mpii.microblogtrack.component.features;

import gnu.trove.map.TLongDoubleMap;
import gnu.trove.map.hash.TLongDoubleHashMap;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.Callable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import twitter4j.Status;

/**
 * based on org.apache.lucene.demo
 *
 * @author khui
 */
public class LuceneScores {

    private final IndexWriter writer;

    private final IndexReader reader;

    private final IndexSearcher searcher;

    private final ExtractTweetText textextractor;

    public LuceneScores(String indexdir) throws IOException {
        Directory dir = FSDirectory.open(Paths.get(indexdir));
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
        iwc.setRAMBufferSizeMB(1024.0 * 5);
        this.writer = new IndexWriter(dir, iwc);
        this.reader = DirectoryReader.open(writer, false);
        this.searcher = new IndexSearcher(reader);
        this.textextractor = new ExtractTweetText();
    }

    private HashMap<String, String> status2Fields(Status status) {
        HashMap<String, String> fieldnameStr = new HashMap<>();
        String tweeturl = textextractor.getExpanded(status);
        fieldnameStr.put("tweeturl", tweeturl);
        return fieldnameStr;
    }

    /**
     * Indexes a single document
     *
     * @param tweet
     * @throws java.io.IOException
     */
    public void indexDoc(Status tweet) throws IOException {
        HashMap<String, String> fieldContent = status2Fields(tweet);
        long tweetid = tweet.getId();
        Document doc = new Document();
        doc.add(new LongField("tweetid", tweetid, Field.Store.NO));
        for (String fieldname : fieldContent.keySet()) {
            doc.add(new TextField(fieldname, fieldContent.get(fieldname), Field.Store.NO));
        }
        writer.addDocument(doc);
    }

    public void closeIndexWriter() throws IOException {
        writer.close();
    }

    public void nrtSearch(int topN, long startId, long endId, Query termquery) throws IOException {
        BooleanQuery combinedQuery = new BooleanQuery();
        combinedQuery.add(termquery, BooleanClause.Occur.SHOULD);
        NumericRangeQuery tweetidrg = NumericRangeQuery.newLongRange("tweetid", startId, endId, true, true);
        combinedQuery.add(tweetidrg, BooleanClause.Occur.MUST);
        TopDocs topdocs = searcher.search(combinedQuery, topN);
        ScoreDoc[] hits = topdocs.scoreDocs;
        for (ScoreDoc sdoc : hits) {
            Document tweet = searcher.doc(sdoc.doc);
            long tweetid = Long.parseLong(tweet.get("tweetid"));
            System.out.println(tweetid + ":" + sdoc.score);
        }
    }

    public class NRTSearch implements Callable<TLongDoubleMap> {

        private final int topN;

        private final long startId;

        private final long endId;

        private final Query termquery;

        public NRTSearch(int topN, long startId, long endId, Query termquery) {
            this.topN = topN;
            this.startId = startId;
            this.endId = endId;
            this.termquery = termquery;
        }

        @Override
        public TLongDoubleMap call() throws Exception {
            BooleanQuery combinedQuery = new BooleanQuery();
            combinedQuery.add(termquery, BooleanClause.Occur.SHOULD);
            NumericRangeQuery tweetidrg = NumericRangeQuery.newLongRange("tweetid", startId, endId, true, true);
            combinedQuery.add(tweetidrg, BooleanClause.Occur.MUST);
            TopDocs topdocs = searcher.search(combinedQuery, topN);
            ScoreDoc[] hits = topdocs.scoreDocs;
            TLongDoubleMap tidScore = new TLongDoubleHashMap();
            for (ScoreDoc sdoc : hits) {
                Document tweet = searcher.doc(sdoc.doc);
                long tweetid = Long.parseLong(tweet.get("tweetid"));
                tidScore.put(tweetid, sdoc.score);
                System.out.println(tweetid + ":" + sdoc.score);
            }
            return tidScore;
        }
    }

}
