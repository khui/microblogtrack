package de.mpii.microblogtrack.component;

import de.mpii.microblogtrack.component.filter.Filter;
import de.mpii.microblogtrack.component.filter.LangFilterTW;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
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
import org.apache.lucene.index.IndexableField;
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
 * core class: index the incoming tweet, and periodically retrieve top tweets
 * for each query for further processing.
 *
 * usage: pass status for indexing, the output is query, tweet pairs stored in a
 * blocking queue
 *
 * TODO: better lang detector, tokenizer
 *
 * @author khui
 */
public class LuceneScorer {

    static Logger logger = Logger.getLogger(LuceneScorer.class.getName());

    private final IndexWriter writer;

    private IndexReader reader;

    private IndexSearcher searcher;
    // track duplicate tweet and allocate unique tweetCountId to each received tweet
    private final IndexTracker indexTracker;

    private final ExtractTweetText textextractor;
    // language filter, retaining english tweets
    private final Filter langfilter;

    public LuceneScorer(String indexdir) throws IOException {
        Directory dir = FSDirectory.open(Paths.get(indexdir));
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(OpenMode.CREATE);
        //iwc.setRAMBufferSizeMB(1024.0 * 5);
        this.writer = new IndexWriter(dir, iwc);

        //this.searcher = new IndexSearcher(reader);
        this.textextractor = new ExtractTweetText();
        this.indexTracker = new IndexTracker();
        this.langfilter = new LangFilterTW();
    }

    public void multiQuerySearch(final BlockingQueue<QueryTweetPair> bq, final Map<String, Query> queries) throws IOException, InterruptedException, ExecutionException {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        final ScheduledFuture<?> sercherHandler = scheduler.scheduleAtFixedRate(new MultiQuerySearcher(queries, bq), 60, 60, TimeUnit.SECONDS);
        // the task will be canceled after running 30 days automatically
        scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                sercherHandler.cancel(true);
            }
        }, 30, TimeUnit.DAYS);
    }

    /**
     * for debug
     *
     * @throws java.io.IOException
     */
    public void readIndex() throws IOException {
        writer.commit();
        this.reader = DirectoryReader.open(writer, false);
        int docnum = writer.numDocs();
        logger.info("readIndex:" + docnum);
        for (int i = 0; i < docnum; i++) {
            Document doc = reader.document(i);
            List<IndexableField> fields = doc.getFields();
            for (IndexableField f : fields) {
                logger.info(f.name() + ":" + f.stringValue());
            }
        }
    }

    public int getIndexSize() {
        return writer.numDocs();
    }

    public void closeWriter() throws IOException {
        writer.close();
    }

    public void commmitChange() throws IOException {
        logger.info("Committing changes.");
        writer.commit();
    }

    public long write2Index(Status tweet) throws IOException {
        boolean isEng = langfilter.isRetain(null, null, tweet);
        if (isEng) {
            long tweetcountId = indexTracker.isDuplicate(null, null, tweet);
            if (tweetcountId > 0) {
                HashMap<String, String> fieldContent = status2Fields(tweet);
                Document doc = new Document();
                doc.add(new LongField("tweetcountid", tweetcountId, Field.Store.YES));
                for (String fieldname : fieldContent.keySet()) {
                    doc.add(new TextField(fieldname, fieldContent.get(fieldname), Field.Store.YES));
                }
                writer.addDocument(doc);
                return tweetcountId;
            }
        }
        return -1;
    }

    private HashMap<String, String> status2Fields(Status status) {
        HashMap<String, String> fieldnameStr = new HashMap<>();
        String tweeturl = textextractor.getTweet(status);
        fieldnameStr.put("tweeturl", tweeturl);
        return fieldnameStr;

    }

    private class MultiQuerySearcher implements Runnable {

        private final NumericRangeQuery rangeQuery;

        private final Map<String, Query> queries;

        private final BlockingQueue<QueryTweetPair> qtweetpairs;

        public MultiQuerySearcher(final Map<String, Query> queries, BlockingQueue<QueryTweetPair> bq) {
            this.queries = queries;
            this.qtweetpairs = bq;
            long[] minmax = indexTracker.getAcurateTweetCount();
            this.rangeQuery = NumericRangeQuery.newLongRange("tweetcountid", minmax[0], minmax[1], true, true);
        }

        @Override
        public void run() {
            try {
                LuceneScorer.this.commmitChange();
            } catch (IOException ex) {
                logger.error(ex.getMessage());
            }
            BooleanQuery combinedQuery;
            ExecutorService service = Executors.newFixedThreadPool(10);

            for (String queryid : queries.keySet()) {
                combinedQuery = new BooleanQuery();
                combinedQuery.add(queries.get(queryid), BooleanClause.Occur.SHOULD);
                combinedQuery.add(this.rangeQuery, BooleanClause.Occur.MUST);
                service.submit(new UniqQuerySearcher(combinedQuery, queryid, qtweetpairs));
            }
            service.shutdown();
            try {
                boolean isFinished = service.awaitTermination(50, TimeUnit.SECONDS);
                logger.info("Retrieval task finished: " + isFinished);
            } catch (InterruptedException ie) {
                logger.error(ie.getMessage());
            }
        }
    }

    private class UniqQuerySearcher implements Callable<Void> {

        private int topN = 5;

        private final String queryid;

        private final Query query;

        private final BlockingQueue<QueryTweetPair> querytweetpairs;

        public UniqQuerySearcher(Query query, String queryId, BlockingQueue<QueryTweetPair> bq) {
            this.query = query;
            this.queryid = queryId;
            this.querytweetpairs = bq;
        }

        public void setTopN(int topN) {
            this.topN = topN;
        }

        @Override
        public Void call() throws Exception {
            TopDocs topdocs = searcher.search(query, topN);
            ScoreDoc[] hits = topdocs.scoreDocs;
            QueryTweetPair qtp;
            for (ScoreDoc sdoc : hits) {
                Document tweet = searcher.doc(sdoc.doc);
                long tweetid = Long.parseLong(tweet.get("tweetid"));
                qtp = new QueryTweetPair(tweetid, queryid, indexTracker.getStatus(tweetid));
                qtp.updateFeatures("tfidf", sdoc.score);
                querytweetpairs.offer(new QueryTweetPair(qtp), 60, TimeUnit.SECONDS);
                logger.info(tweetid + ":" + sdoc.score);
            }
            return null;
        }
    }

}
