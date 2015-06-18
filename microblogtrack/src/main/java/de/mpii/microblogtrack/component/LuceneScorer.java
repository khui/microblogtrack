package de.mpii.microblogtrack.component;

import de.mpii.microblogtrack.component.filter.Filter;
import de.mpii.microblogtrack.component.filter.LangFilterTW;
import de.mpii.microblogtrack.userprofiles.TrecQuery;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.HashMap;
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
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.util.QueryBuilder;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
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

    private DirectoryReader directoryReader;

    private final Analyzer analyzer;

    // track duplicate tweet and allocate unique tweetCountId to each received tweet
    private final IndexTracker indexTracker;

    private final ExtractTweetText textextractor;
    // language filter, retaining english tweets
    private final Filter langfilter;

    private static final String[] searchModels = new String[]{"tfidf", "bm25", "lmd"};

    public LuceneScorer(String indexdir) throws IOException {
        Directory dir = FSDirectory.open(Paths.get(indexdir));
        this.analyzer = new EnglishAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(OpenMode.CREATE);
        iwc.setRAMBufferSizeMB(1024.0 * 5);
        this.writer = new IndexWriter(dir, iwc);
        this.directoryReader = DirectoryReader.open(writer, false);
        this.textextractor = new ExtractTweetText();
        this.indexTracker = new IndexTracker();
        this.langfilter = new LangFilterTW();

    }

    public void multiQuerySearch(final BlockingQueue<QueryTweetPair> bq, String queryfile) throws IOException, InterruptedException, ExecutionException, ParseException {
        TrecQuery tq = new TrecQuery();
        Map<String, Query> queries = tq.readInQueries(queryfile, this.analyzer, "tweeturl");
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

    private Collection<QueryTweetPair> mutliScorers(IndexReader reader, Query query, String queryId, int topN) throws IOException {
        TLongObjectMap<QueryTweetPair> searchresults = new TLongObjectHashMap<>();
        IndexSearcher searcherInUse;
        ScoreDoc[] hits;
        Document tweet;
        long tweetid;
        for (String name : searchModels) {
            searcherInUse = new IndexSearcher(reader);
            switch (name) {
                case "tfidf":
                    break;
                case "bm25":
                    searcherInUse.setSimilarity(new BM25Similarity());
                    break;
                case "lmd":
                    searcherInUse.setSimilarity(new LMDirichletSimilarity());
                    break;
            }
            hits = searcherInUse.search(query, topN).scoreDocs;
            for (ScoreDoc hit : hits) {
                tweet = searcherInUse.doc(hit.doc);
                tweetid = Long.parseLong(tweet.get("tweetid"));
                if (!searchresults.containsKey(tweetid)) {
                    searchresults.put(tweetid, new QueryTweetPair(tweetid, queryId, indexTracker.getStatus(tweetid)));
                }
                searchresults.get(tweetid).updateFeatures(name, hit.score);
            }
        }
        return searchresults.valueCollection();
    }

    /**
     * for debug
     *
     * @throws java.io.IOException
     */
    public void multiScorerDemo() throws IOException {
        NumericRangeQuery rangeQuery;
        int resultcount = 0;
        Collection<QueryTweetPair> qtpairs;
        directoryReader = DirectoryReader.openIfChanged(directoryReader);
        QueryBuilder qb = new QueryBuilder(this.analyzer);
        Query termquery = qb.createBooleanQuery("tweeturl", "RT");
        Query phrasequery = qb.createPhraseQuery("tweeturl", "thanks for");
        long[] minmax = indexTracker.getAcurateTweetCount();
        rangeQuery = NumericRangeQuery.newLongRange("tweetcountid", minmax[0], minmax[1], true, false);
        BooleanQuery combinedQuery = new BooleanQuery();
        combinedQuery.add(rangeQuery, BooleanClause.Occur.MUST);
        combinedQuery.add(termquery, BooleanClause.Occur.SHOULD);
        combinedQuery.add(phrasequery, BooleanClause.Occur.SHOULD);
        logger.info("***************************************************************************");
        logger.info("***************************************************************************");
        logger.info("indexed documents: " + minmax[0] + " -------- " + (minmax[1] - 1));
        if (directoryReader != null) {
            qtpairs = mutliScorers(directoryReader, combinedQuery, "queryid", 10);
            for (QueryTweetPair qtp : qtpairs) {
                resultcount++;
                logger.info(printQueryTweet(qtp, resultcount));
            }
        }
    }

    /**
     * for debug
     *
     * @param qtp
     * @param resultcount
     * @return
     */
    public static String printQueryTweet(QueryTweetPair qtp, int resultcount) {
        DecimalFormat df = new DecimalFormat("#.000");
        StringBuilder sb = new StringBuilder();
        sb.append("#").append(resultcount).append(" ");
        sb.append(qtp.queryid).append("--").append(qtp.tweetid).append(" ");
        for (String name : searchModels) {
            sb.append(name).append(":").append(df.format(qtp.getFeature(name))).append(",");
        }
        sb.append(" ").append(qtp.getStatus().getText());
        return sb.toString();
    }

    public int getIndexSize() {
        return writer.numDocs();
    }

    public void closeWriter() throws IOException {
        writer.close();
    }

    public long write2Index(Status tweet) throws IOException {
        boolean isEng = langfilter.isRetain(null, null, tweet);
        if (isEng) {
            long tweetcountId = indexTracker.isDuplicate(null, null, tweet);
            if (tweetcountId > 0) {
                HashMap<String, String> fieldContent = status2Fields(tweet);
                Document doc = new Document();
                doc.add(new LongField("tweetcountid", tweetcountId, Field.Store.YES));
                doc.add(new LongField("tweetid", tweet.getId(), Field.Store.YES));
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

        private final Map<String, Query> queries;

        private final BlockingQueue<QueryTweetPair> qtweetpairs;

        private int threadnum = 12;

        public MultiQuerySearcher(final Map<String, Query> queries, BlockingQueue<QueryTweetPair> bq) {
            this.queries = queries;
            this.qtweetpairs = bq;
        }

        public void setThreadNum(int threadnum) {
            this.threadnum = threadnum;
        }

        @Override
        public void run() {
            BooleanQuery combinedQuery;
            ExecutorService service = Executors.newFixedThreadPool(threadnum);
            long[] minmax = indexTracker.getAcurateTweetCount();
            NumericRangeQuery rangeQuery = NumericRangeQuery.newLongRange("tweetcountid", minmax[0], minmax[1], true, true);
            DirectoryReader reopenedReader = null;
            try {
                reopenedReader = DirectoryReader.openIfChanged(directoryReader);
            } catch (IOException ex) {
                logger.error(ex.getMessage());
            }
            if (reopenedReader != null) {
                try {
                    directoryReader.close();
                } catch (IOException ex) {
                    logger.error(ex.getMessage());
                }
                directoryReader = reopenedReader;
                for (String queryid : queries.keySet()) {
                    combinedQuery = new BooleanQuery();
                    combinedQuery.add(queries.get(queryid), BooleanClause.Occur.SHOULD);
                    combinedQuery.add(rangeQuery, BooleanClause.Occur.MUST);
                    service.submit(new UniqQuerySearcher(combinedQuery, queryid, qtweetpairs, directoryReader));
                }
                service.shutdown();
                try {
                    boolean isFinished = service.awaitTermination(50, TimeUnit.SECONDS);
                    logger.info("Retrieval task finished: " + isFinished);
                } catch (InterruptedException ie) {
                    logger.error(ie.getMessage());
                }
            } else {
                logger.warn("Nothing added to the index since last open of reader.");
            }
        }
    }

    private class UniqQuerySearcher implements Callable<Void> {

        private int topN = 2;

        private final String queryid;

        private final Query query;

        private final BlockingQueue<QueryTweetPair> querytweetpairs;

        private final DirectoryReader reader;

        public UniqQuerySearcher(Query query, String queryId, BlockingQueue<QueryTweetPair> bq, DirectoryReader reader) {
            this.query = query;
            this.queryid = queryId;
            this.querytweetpairs = bq;
            this.reader = reader;
        }

        public void setTopN(int topN) {
            this.topN = topN;
        }

        @Override
        public Void call() throws Exception {
            Collection<QueryTweetPair> qtpairs = mutliScorers(this.reader, this.query, this.queryid, this.topN);
            int resultcount = 1;
            for (QueryTweetPair qtp : qtpairs) {
                querytweetpairs.offer(new QueryTweetPair(qtp), 60, TimeUnit.SECONDS);
                //logger.info(printQueryTweet(qtp, resultcount));
                resultcount++;
            }
            return null;
        }
    }

}
