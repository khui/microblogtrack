package de.mpii.microblogtrack.component;

import de.mpii.microblogtrack.component.filter.Filter;
import de.mpii.microblogtrack.component.filter.LangFilterTW;
import de.mpii.microblogtrack.component.predictor.PointwiseScorer;
import de.mpii.microblogtrack.userprofiles.TrecQuery;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import gnu.trove.map.TLongObjectMap;
import de.mpii.microblogtrack.utility.Configuration;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
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

    private final Map<String, ResultTweetsTracker> queryResultTrackers;

    private static final String[] searchModels = Configuration.FEATURES_SEMANTIC;

    private final PointwiseScorer pwScorer;

    private final Map<String, double[]> featureMeanStd;

    public LuceneScorer(String indexdir, Map<String, ResultTweetsTracker> queryTweetList, PointwiseScorer pwScorer, Map<String, double[]> featureMeanStd) throws IOException {
        Directory dir = FSDirectory.open(Paths.get(indexdir));
        this.analyzer = new EnglishAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(OpenMode.CREATE);
        iwc.setRAMBufferSizeMB(Configuration.LUCENE_MEM_SIZE);
        this.writer = new IndexWriter(dir, iwc);
        this.directoryReader = DirectoryReader.open(writer, false);
        this.textextractor = new ExtractTweetText();
        this.indexTracker = new IndexTracker();
        this.langfilter = new LangFilterTW();
        this.queryResultTrackers = queryTweetList;
        this.pwScorer = pwScorer;
        this.featureMeanStd = featureMeanStd;
    }

    public void multiQuerySearch(String queryfile, BlockingQueue<QueryTweetPair> queue2offer4PW, BlockingQueue<QueryTweetPair> queue2offer4LW) throws IOException, InterruptedException, ExecutionException, ParseException {
        TrecQuery tq = new TrecQuery();
        Map<String, Query> queries = tq.readInQueries(queryfile, this.analyzer, Configuration.TWEET_CONTENT);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        final ScheduledFuture<?> sercherHandler = scheduler.scheduleAtFixedRate(new MultiQuerySearcher(queries, queue2offer4PW, queue2offer4LW), Configuration.LUCENE_SEARCH_FREQUENCY, Configuration.LUCENE_SEARCH_FREQUENCY, TimeUnit.SECONDS);
        // the task will be canceled after running certain days automatically
        scheduler.schedule(() -> {
            sercherHandler.cancel(true);
        }, 12, TimeUnit.DAYS);
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
        Query termquery = qb.createBooleanQuery(Configuration.TWEET_CONTENT, "RT");
        Query phrasequery = qb.createPhraseQuery(Configuration.TWEET_CONTENT, "thanks for");
        long[] minmax = indexTracker.getAcurateTweetCount();
        rangeQuery = NumericRangeQuery.newLongRange(Configuration.TWEET_COUNT, minmax[0], minmax[1], true, false);
        BooleanQuery combinedQuery = new BooleanQuery();
        combinedQuery.add(rangeQuery, BooleanClause.Occur.MUST);
        combinedQuery.add(termquery, BooleanClause.Occur.SHOULD);
        combinedQuery.add(phrasequery, BooleanClause.Occur.SHOULD);
        logger.info("***************************************************************************");
        logger.info("***************************************************************************");
        logger.info("indexed documents: " + minmax[0] + " -------- " + (minmax[1] - 1));
        if (directoryReader != null) {
            qtpairs = null;
            //mutliScorers(directoryReader, combinedQuery, Configuration.QUERY_ID, 10);
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
        for (String name : Configuration.FEATURES_SEMANTIC) {
            sb.append(name).append(":").append(df.format(qtp.getFeature(name))).append(",");
        }
        sb.append(" ").append(qtp.getStatus().getText());
        return sb.toString();
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
                doc.add(new LongField(Configuration.TWEET_COUNT, tweetcountId, Field.Store.YES));
                doc.add(new LongField(Configuration.TWEET_ID, tweet.getId(), Field.Store.YES));
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
        String str = textextractor.getTweet(status);
        fieldnameStr.put(Configuration.TWEET_CONTENT, str);
        return fieldnameStr;

    }

    private class MultiQuerySearcher implements Runnable {

        private final Map<String, Query> queries;

        private int threadnum = Configuration.LUCENE_SEARCH_THREADNUM;

        private final BlockingQueue<QueryTweetPair> queue2offer4PW;

        private final BlockingQueue<QueryTweetPair> queue2offer4LW;

        public MultiQuerySearcher(final Map<String, Query> queries, BlockingQueue<QueryTweetPair> queue2offer4PW, BlockingQueue<QueryTweetPair> queue2offer4LW) {
            this.queries = queries;
            this.queue2offer4PW = queue2offer4PW;
            this.queue2offer4LW = queue2offer4LW;
        }

        public void setThreadNum(int threadnum) {
            this.threadnum = threadnum;
        }

        @Override
        public void run() {
            BooleanQuery combinedQuery;
            Executor excutor = Executors.newFixedThreadPool(threadnum);
            CompletionService<UniqQuerySearchResult> completeservice = new ExecutorCompletionService<>(excutor);
            long[] minmax = indexTracker.getAcurateTweetCount();
            NumericRangeQuery rangeQuery = NumericRangeQuery.newLongRange(Configuration.TWEET_COUNT, minmax[0], minmax[1], true, false);
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
                //////////////////////////////////////
                // for test
                NumericRangeQuery boundaryTest = NumericRangeQuery.newLongRange(Configuration.TWEET_COUNT, minmax[1] - 1, minmax[1] - 1, true, true);
                BooleanQuery bq = new BooleanQuery();
                bq.add(boundaryTest, BooleanClause.Occur.MUST);
                try {
                    ScoreDoc[] docs = new IndexSearcher(directoryReader).search(bq, 1).scoreDocs;
                    if (docs.length == 0) {
                        logger.error("!! the boundary is not visible for the searcher:" + (minmax[1] - 1));
                    }
                } catch (IOException ex) {
                    logger.error(ex.getMessage());
                }
                // end test
                ///////////////////////////////////////
                for (String queryid : queries.keySet()) {
                    if (!queryResultTrackers.containsKey(queryid)) {
                        try {
                            queryResultTrackers.put(queryid, new ResultTrackerKMean(queryid));
                        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
                            logger.error("multiquery search", ex);
                        }
                    }
                    combinedQuery = new BooleanQuery();
                    combinedQuery.add(queries.get(queryid), BooleanClause.Occur.SHOULD);
                    combinedQuery.add(rangeQuery, BooleanClause.Occur.MUST);
                    completeservice.submit(new UniqQuerySearcher(combinedQuery, queryid, directoryReader));
                }
                int resultnum = queries.size();
                UniqQuerySearchResult queryranking;
                for (int i = 0; i < resultnum; ++i) {
                    try {
                        final Future<UniqQuerySearchResult> futureQtpCollection = completeservice.take();
                        queryranking = futureQtpCollection.get();
                        if (queryranking != null) {
                            // update the result tracker
                            queryResultTrackers.get(queryranking.queryid).addTweets(queryranking.results);
                            if (queryResultTrackers.get(queryranking.queryid).isStarted()) {
                                queryranking.offer2queue(queue2offer4PW, queue2offer4LW);
                            }
                        } else {
                            logger.error("queryranking is null.");
                        }

                    } catch (ExecutionException | InterruptedException ex) {
                        logger.error("pass qtp", ex);
                    }
                }
            } else {
                logger.warn("Nothing added to the index since last open of reader.");
            }
        }

    }

    private class UniqQuerySearcher implements Callable<UniqQuerySearchResult> {

        private int topN = Configuration.LUCENE_TOP_N_SEARCH;

        private final String queryid;

        private final Query query;

        private final DirectoryReader reader;

        public UniqQuerySearcher(Query query, String queryId, DirectoryReader reader) {
            this.query = query;
            this.queryid = queryId;
            this.reader = reader;
        }

        public void setTopN(int topN) {
            this.topN = topN;
        }

        private UniqQuerySearchResult mutliScorers(IndexReader reader, Query query, String queryId, int topN) throws IOException {
            TLongObjectMap<QueryTweetPair> searchresults = new TLongObjectHashMap<>();
            IndexSearcher searcherInUse;
            ScoreDoc[] hits;
            Document tweet;
            long tweetid;
            for (String name : searchModels) {
                searcherInUse = new IndexSearcher(reader);
                switch (name) {
                    case Configuration.FEATURE_S_TFIDF:
                        break;
                    case Configuration.FEATURE_S_BM25:
                        searcherInUse.setSimilarity(new BM25Similarity());
                        break;
                    case Configuration.FEATURE_S_LMD:
                        searcherInUse.setSimilarity(new LMDirichletSimilarity());
                        break;
                    case Configuration.FEATURE_S_LMJM:
                        searcherInUse.setSimilarity(new LMJelinekMercerSimilarity(Configuration.FEATURE_S_LMJM_Lambda));
                        break;
                }
                hits = searcherInUse.search(query, topN).scoreDocs;
                for (ScoreDoc hit : hits) {
                    tweet = searcherInUse.doc(hit.doc);
                    tweetid = Long.parseLong(tweet.get(Configuration.TWEET_ID));
                    if (!searchresults.containsKey(tweetid)) {
                        searchresults.put(tweetid, new QueryTweetPair(tweetid, queryId, indexTracker.getStatus(tweetid)));
                    }
                    searchresults.get(tweetid).updateFeatures(name, hit.score);
                }
            }
            /**
             * 1) update the minmax tracker in the result tracker 2) re-scale
             * the feature value 3) conduct pointwise prediction
             */
            for (QueryTweetPair qtp : searchresults.valueCollection()) {
                qtp.rescaleFeatures(featureMeanStd);
                pwScorer.predictor(qtp);
                qtp.vectorizeMahout();
            }
            return new UniqQuerySearchResult(queryid, searchresults.valueCollection());
        }

        @Override
        public UniqQuerySearchResult call() throws Exception {
            UniqQuerySearchResult qtpairs = mutliScorers(this.reader, this.query, this.queryid, this.topN);
            return qtpairs;
        }

    }

    private class UniqQuerySearchResult {

        public final String queryid;
        public final Collection<QueryTweetPair> results;

        private UniqQuerySearchResult(String queryid, Collection<QueryTweetPair> results) {
            this.queryid = queryid;
            this.results = results;
        }

        private void offer2queue(BlockingQueue<QueryTweetPair> queue) throws InterruptedException {
            for (QueryTweetPair qtp : results) {
                // offer to the blocking queue for the decision maker
                boolean isSucceed = queue.offer(new QueryTweetPair(qtp), 100, TimeUnit.MILLISECONDS);
                if (!isSucceed) {
                    logger.error("offer to queue failed.");
                }
            }
        }

        private void offer2queue(BlockingQueue<QueryTweetPair> queue2offer4PW, BlockingQueue<QueryTweetPair> queue2offer4LW) throws InterruptedException {
            for (QueryTweetPair qtp : results) {
                // offer to the blocking queue for the pointwise decision maker
                boolean isSucceed = queue2offer4PW.offer(new QueryTweetPair(qtp), 100, TimeUnit.MILLISECONDS);
                if (!isSucceed) {
                    logger.error("offer to queue2offer4PW failed.");
                }
                // offer to the blocking queue for the decision maker
                isSucceed = queue2offer4LW.offer(new QueryTweetPair(qtp), 100, TimeUnit.MILLISECONDS);
                if (!isSucceed) {
                    logger.error("offer to queue2offer4LW failed.");
                }
            }
        }

    }

}
