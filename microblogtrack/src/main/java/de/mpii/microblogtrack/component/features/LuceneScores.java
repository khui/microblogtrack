package de.mpii.microblogtrack.component.features;

import de.mpii.microblogtrack.component.filter.DuplicateTweet;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
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

    private final DuplicateTweet duplicateDetector;

    public LuceneScores(String indexdir, DuplicateTweet duplicateDetector) throws IOException {
        Directory dir = FSDirectory.open(Paths.get(indexdir));
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
        iwc.setRAMBufferSizeMB(1024.0 * 5);
        this.writer = new IndexWriter(dir, iwc);
        this.reader = DirectoryReader.open(writer, false);
        this.searcher = new IndexSearcher(reader);
        this.textextractor = new ExtractTweetText();
        this.duplicateDetector = duplicateDetector;
    }

    private HashMap<String, String> status2Fields(Status status) {
        HashMap<String, String> fieldnameStr = new HashMap<>();
        String tweeturl = textextractor.getExpanded(status);
        fieldnameStr.put("tweeturl", tweeturl);
        return fieldnameStr;
    }

    public int getIndexSize() {
        return reader.numDocs();
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

    public class NRTSearch implements Callable<Void> {

        private int topN = 5;

        private final String queryId;

        private final BooleanQuery combinedQuery = new BooleanQuery();

        private final BlockingQueue<QueryTweetPair> querytweetpairs;

        public NRTSearch(long[] minmaxId, Query termquery, String queryId, BlockingQueue<QueryTweetPair> querytweetpairs) {
            this.combinedQuery.add(termquery, BooleanClause.Occur.SHOULD);
            this.combinedQuery.add(NumericRangeQuery.newLongRange("tweetid", minmaxId[0], minmaxId[1], true, true), BooleanClause.Occur.MUST);
            this.queryId = queryId;
            this.querytweetpairs = querytweetpairs;
        }

        public void setTopN(int topN) {
            this.topN = topN;
        }

        @Override
        public Void call() throws Exception {
            TopDocs topdocs = searcher.search(combinedQuery, topN);
            ScoreDoc[] hits = topdocs.scoreDocs;
            QueryTweetPair qtp;
            for (ScoreDoc sdoc : hits) {
                Document tweet = searcher.doc(sdoc.doc);
                long tweetid = Long.parseLong(tweet.get("tweetid"));
                qtp = new QueryTweetPair(tweetid, queryId, duplicateDetector.getStatus(tweetid));
                qtp.updateFeatures("tfidf", sdoc.score);
                querytweetpairs.offer(new QueryTweetPair(qtp), 60, TimeUnit.SECONDS);
                System.out.println(tweetid + ":" + sdoc.score);
            }
            return null;
        }
    }

}
