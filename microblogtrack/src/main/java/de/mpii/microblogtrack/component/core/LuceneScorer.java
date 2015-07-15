package de.mpii.microblogtrack.component.core;

import de.mpii.microblogtrack.component.ExtractTweetText;
import de.mpii.microblogtrack.component.ExtractTweetText.TweetidUrl;
import de.mpii.microblogtrack.component.IndexTracker;
import de.mpii.microblogtrack.component.TweetStringSimilarity;
import de.mpii.microblogtrack.component.filter.Filter;
import de.mpii.microblogtrack.component.filter.LangFilterTW;
import de.mpii.microblogtrack.component.predictor.PointwiseScorer;
import de.mpii.microblogtrack.component.predictor.PointwiseScorerSumRetrievalScores;
import de.mpii.microblogtrack.task.offline.qe.ExpandQueryWithWiki;
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
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.AfterEffectB;
import org.apache.lucene.search.similarities.AfterEffectL;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BasicModelBE;
import org.apache.lucene.search.similarities.BasicModelIF;
import org.apache.lucene.search.similarities.DFRSimilarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Normalization;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import twitter4j.Status;
import twitter4j.URLEntity;

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

    static final FieldType TEXT_OPTIONS = new FieldType();

    static {
        TEXT_OPTIONS.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        TEXT_OPTIONS.setStored(true);
        TEXT_OPTIONS.setTokenized(true);
    }

    private final IndexWriter writer;

    private DirectoryReader directoryReader;

    private final Analyzer analyzer;

    // track duplicate tweet and allocate unique tweetCountId to each received tweet
    private final IndexTracker indexTracker;

    private final ExtractTweetText textextractor;
    // language filter, retaining english tweets
    private final Filter langfilter;

    private final Map<String, LuceneDMConnector> relativeScoreTracker;

    private final PointwiseScorer pwScorer;

    public LuceneScorer(String indexdir, Map<String, LuceneDMConnector> queryTweetList, PointwiseScorerSumRetrievalScores pwScorer) throws IOException {
        Directory dir = FSDirectory.open(Paths.get(indexdir));
        this.analyzer = new EnglishAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(OpenMode.CREATE);
        iwc.setRAMBufferSizeMB(Configuration.LUCENE_MEM_SIZE);
        this.writer = new IndexWriter(dir, iwc);
        this.directoryReader = DirectoryReader.open(writer, false);
        this.textextractor = new ExtractTweetText(Configuration.LUCENE_DOWNLOAD_URL_TIMEOUT);
        this.indexTracker = new IndexTracker();
        this.langfilter = new LangFilterTW();
        this.relativeScoreTracker = queryTweetList;
        this.pwScorer = pwScorer;
    }

    public void multiQuerySearch(String queryfile, String expandqueryfile, BlockingQueue<QueryTweetPair> queue2offer4PW, BlockingQueue<QueryTweetPair> queue2offer4LW) throws IOException, InterruptedException, ExecutionException, ParseException {
        Map<String, Map<String, Query>> qidFieldQuery = prepareQuery(queryfile);
        Map<String, Map<String, Query>> eqidFieldQuery = prepareExpandedQuery(expandqueryfile);
        logger.info("read in query and expanded query sizes/types: " + qidFieldQuery.size());
        // initialize trackers: track the centroids, the relative score
        for (String queryid : qidFieldQuery.keySet()) {
            if (!relativeScoreTracker.containsKey(queryid)) {
                relativeScoreTracker.put(queryid, new LuceneDMConnector(queryid));
            }
        }
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        final ScheduledFuture<?> sercherHandler = scheduler.scheduleAtFixedRate(new MultiQuerySearcher(qidFieldQuery, eqidFieldQuery, queue2offer4PW, queue2offer4LW), Configuration.LUCENE_SEARCH_FREQUENCY, Configuration.LUCENE_SEARCH_FREQUENCY, TimeUnit.SECONDS);
        // the task will be canceled after running certain days automatically
        scheduler.schedule(() -> {
            sercherHandler.cancel(true);
        }, 12, TimeUnit.DAYS);
    }

    private Map<String, Map<String, Query>> prepareQuery(String queryfile) throws IOException, ParseException {
        TrecQuery tq = new TrecQuery();
        Map<String, Map<String, Query>> qidFieldQuery = tq.readFieldQueries(queryfile, analyzer);
        return qidFieldQuery;
    }

    private Map<String, Map<String, Query>> prepareExpandedQuery(String expandfile) throws IOException {
        Map<String, Map<String, Query>> qidFieldQuery = ExpandQueryWithWiki.readExpandedFieldQueries(expandfile, analyzer, Configuration.QUERY_EXPANSION_TERMNUM);
        return qidFieldQuery;
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
        for (String name : Configuration.FEATURES_RETRIVEMODELS) {
            sb.append(name).append(":").append(df.format(qtp.getFeature(name))).append(",");
        }
        sb.append(" ").append(qtp.getTweetText());
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
                    doc.add(new Field(fieldname, fieldContent.get(fieldname), TEXT_OPTIONS));
                }
                writer.addDocument(doc);
                return tweetcountId;
            }
        }
        return -1;
    }

    private HashMap<String, String> status2Fields(Status status) throws IOException {
        HashMap<String, String> fieldnameStr = new HashMap<>();
        String tweetcontent = textextractor.getTweet(status);
        // String tweeturltitle = textextractor.getUrlTitle(status);
        fieldnameStr.put(Configuration.TWEET_CONTENT, tweetcontent);
        //fieldnameStr.put(Configuration.TWEET_URL_TITLE, tweeturltitle);
        return fieldnameStr;

    }

    private class MultiQuerySearcher implements Runnable {

        private final Map<String, Map<String, Query>> qidFieldQuery;

        private final Map<String, Map<String, Query>> eqidFieldQuery;

        private int threadnum = Configuration.LUCENE_SEARCH_THREADNUM;

        private final BlockingQueue<QueryTweetPair> queue2offer4PW;

        private final BlockingQueue<QueryTweetPair> queue2offer4LW;

        private final Executor downloadURLExcutor = Executors.newFixedThreadPool(Configuration.LUCENE_DOWNLOAD_URL_THREADNUM);

        /**
         * to report how many tweets we received in last period
         */
        private int count_runningtime = 0;
        private int count_tweets = 0;

        public MultiQuerySearcher(final Map<String, Map<String, Query>> qidFieldQuery, final Map<String, Map<String, Query>> expandQueries, BlockingQueue<QueryTweetPair> queue2offer4PW, BlockingQueue<QueryTweetPair> queue2offer4LW) {
            this.qidFieldQuery = qidFieldQuery;
            this.eqidFieldQuery = expandQueries;
            this.queue2offer4PW = queue2offer4PW;
            this.queue2offer4LW = queue2offer4LW;
        }

        public void setThreadNum(int threadnum) {
            this.threadnum = threadnum;
        }

        private Map<String, Query> generateQuery(long[] minmax, String queryId) {
            Map<String, Query> querytypeQuery = new HashMap<>();
            NumericRangeQuery rangeQuery = NumericRangeQuery.newLongRange(Configuration.TWEET_COUNT, minmax[0], minmax[1], true, false);
            BooleanQuery combinedQuery;
            for (String querytype : Configuration.QUERY_TYPES) {
                // each query type ultimately corresponds to several features: 
                // like bm25 on tweet_content with expanded query
                combinedQuery = new BooleanQuery();
                combinedQuery.add(rangeQuery, Occur.MUST);
                switch (querytype) {
                    case Configuration.Q_TWEET:
                        combinedQuery.add(qidFieldQuery.get(queryId).get(Configuration.TWEET_CONTENT), Occur.SHOULD);
                        break;
                    case Configuration.Q_URL:
                        combinedQuery.add(qidFieldQuery.get(queryId).get(Configuration.TWEET_URL_TITLE), Occur.SHOULD);
                        break;
                    case Configuration.Q_TWEETURL:
                        combinedQuery.add(qidFieldQuery.get(queryId).get(Configuration.TWEET_CONTENT), Occur.SHOULD);
                        combinedQuery.add(qidFieldQuery.get(queryId).get(Configuration.TWEET_URL_TITLE), Occur.SHOULD);
                        break;
                    case Configuration.QE_TWEET:
                        combinedQuery.add(eqidFieldQuery.get(queryId).get(Configuration.TWEET_CONTENT), Occur.SHOULD);
                        break;
                    case Configuration.QE_URL:
                        combinedQuery.add(eqidFieldQuery.get(queryId).get(Configuration.TWEET_URL_TITLE), Occur.SHOULD);
                        break;
                    case Configuration.QE_TWEETURL:
                        combinedQuery.add(eqidFieldQuery.get(queryId).get(Configuration.TWEET_CONTENT), Occur.SHOULD);
                        combinedQuery.add(eqidFieldQuery.get(queryId).get(Configuration.TWEET_URL_TITLE), Occur.SHOULD);
                        break;
                }
                querytypeQuery.put(querytype, combinedQuery.clone());
            }
            return querytypeQuery;
        }

        @Override
        public void run() {
            if (Thread.interrupted()) {
                logger.info("lucene scorer get interrupted and will stop");
                try {
                    closeWriter();
                } catch (IOException ex) {
                    logger.error("", ex);
                }
                System.exit(0);
            }

            int topk = Configuration.LUCENE_TOP_N_SEARCH;
            Map<String, Query> querytypeQuery;
            Executor excutor = Executors.newFixedThreadPool(threadnum);
            CompletionService<UniqQuerySearchResult> completeservice = new ExecutorCompletionService<>(excutor);
            long[] minmax = indexTracker.minMaxTweetCountInTimeInterval();
            ////////////////////////////
            /// report number of tweets received in the latest period
            count_runningtime++;
            count_tweets += (minmax[1] - minmax[0]);
            if (count_runningtime == Configuration.LW_DM_PERIOD) {
                logger.info(count_tweets + " tweets are written to Lucene index in past " + Configuration.LW_DM_PERIOD + " miniutes.");
                count_runningtime = 0;
                count_tweets = 0;
            }
            ////////////////////////////
            //NumericRangeQuery rangeQuery = NumericRangeQuery.newLongRange(Configuration.TWEET_COUNT, minmax[0], minmax[1], true, false);
            DirectoryReader reopenedReader = null;
            try {
                reopenedReader = DirectoryReader.openIfChanged(directoryReader);
            } catch (IOException ex) {
                logger.error("", ex);
            }
            if (reopenedReader != null) {
                try {
                    directoryReader.close();
                } catch (IOException ex) {
                    logger.error("", ex);
                }
                directoryReader = reopenedReader;
                for (String queryid : qidFieldQuery.keySet()) {
                    querytypeQuery = generateQuery(minmax, queryid);
                    completeservice.submit(new UniqQuerySearcher(querytypeQuery, queryid, directoryReader, downloadURLExcutor, topk));
                }
                int resultnum = qidFieldQuery.size();
                UniqQuerySearchResult queryranking;
                for (int i = 0; i < resultnum; ++i) {
                    try {
                        final Future<UniqQuerySearchResult> futureQtpCollection = completeservice.take();
                        queryranking = futureQtpCollection.get();
                        if (queryranking != null) {
                            // update the result tracker
                            relativeScoreTracker.get(queryranking.queryid).addTweets(queryranking.results);
                            // after the pointwise decision maker start, we start to send 
                            // tweets to pw decision maker, before that, we only send to
                            // the listwise decision maker
                            if (relativeScoreTracker.get(queryranking.queryid).whetherOffer2LWQueue()
                                    && relativeScoreTracker.get(queryranking.queryid).whetherOffer2PWQueue()) {
                                queryranking.offer2queue(queue2offer4PW, queue2offer4LW);
                            } else if (relativeScoreTracker.get(queryranking.queryid).whetherOffer2LWQueue()) {
                                queryranking.offer2queue(queue2offer4LW);
                            }
                        } else {
                            logger.error("queryranking is null.");
                        }

                    } catch (ExecutionException | InterruptedException ex) {
                        logger.error("Write into the queue for DM", ex);
                    }
                }
            } else {
                logger.warn("Nothing added to the index since last open of reader.");
            }
        }

    }

    private class UniqQuerySearcher implements Callable<UniqQuerySearchResult> {

        private final int topN;

        private final String queryid;

        private final Map<String, Query> querytypeQuery;

        private final DirectoryReader reader;

        private final Executor downloadURLExcutor;

        public UniqQuerySearcher(Map<String, Query> querytypeQuery, String queryId, DirectoryReader reader, Executor downloadURLExcutor, int topk) {
            this.querytypeQuery = querytypeQuery;
            this.queryid = queryId;
            this.reader = reader;
            this.topN = topk;
            this.downloadURLExcutor = downloadURLExcutor;
        }

        private UniqQuerySearchResult mutliScorers(IndexReader reader, Map<String, Query> querytypeQuery, String queryId, int topN) throws IOException {
            CompletionService<TweetidUrl> urldownloader = new ExecutorCompletionService<>(downloadURLExcutor);
            TLongObjectMap<QueryTweetPair> searchresults = new TLongObjectHashMap<>();
            IndexSearcher searcherInUse;
            ScoreDoc[] hits;
            Document tweet;
            int urldownloadSubmissionCount = 0;
            long tweetid;
            for (String querytype : Configuration.QUERY_TYPES) {
                for (String model : Configuration.FEATURES_RETRIVEMODELS) {
                    searcherInUse = new IndexSearcher(reader);
                    switch (model) {
                        case Configuration.FEATURE_S_TFIDF:
                            break;
                        case Configuration.FEATURE_S_BM25:
                            searcherInUse.setSimilarity(new BM25Similarity(Configuration.FEATURE_S_BM25_k1, Configuration.FEATURE_S_BM25_b));
                            break;
                        case Configuration.FEATURE_S_LMD:
                            searcherInUse.setSimilarity(new LMDirichletSimilarity(Configuration.FEATURE_S_LMD_mu));
                            break;
                        case Configuration.FEATURE_S_LMJM:
                            searcherInUse.setSimilarity(new LMJelinekMercerSimilarity(Configuration.FEATURE_S_LMJM_Lambda));
                            break;
                        case Configuration.FEATURE_S_DFR_BE_B:
                            searcherInUse.setSimilarity(new DFRSimilarity(new BasicModelBE(), new AfterEffectB(), new Normalization.NoNormalization()));
                            break;
                        case Configuration.FEATURE_S_DFR_IF_L:
                            searcherInUse.setSimilarity(new DFRSimilarity(new BasicModelIF(), new AfterEffectL(), new Normalization.NoNormalization()));
                            break;
                    }
                    hits = searcherInUse.search(querytypeQuery.get(querytype), topN).scoreDocs;
                    for (ScoreDoc hit : hits) {
                        tweet = searcherInUse.doc(hit.doc);
                        tweetid = Long.parseLong(tweet.get(Configuration.TWEET_ID));
                        if (!searchresults.containsKey(tweetid)) {
                            searchresults.put(tweetid, new QueryTweetPair(tweetid, queryId, indexTracker.getStatus(tweetid)));
                            URLEntity[] urlentity = indexTracker.getStatus(tweetid).getURLEntities();
                            if (urlentity != null) {
                                if (urlentity.length > 0) {
                                    String url = urlentity[0].getURL();
                                    final TweetidUrl turl = textextractor.new TweetidUrl(tweetid, url);
                                    urldownloader.submit(new Callable() {

                                        @Override
                                        public TweetidUrl call() throws Exception {
                                            double similarity = 0;
                                            textextractor.getUrlTitle(turl);
                                            if (turl.isAvailable) {
                                                similarity = TweetStringSimilarity.strJarcard(turl.urltitle,
                                                        querytypeQuery.get(Configuration.QE_TWEET).toString(Configuration.TWEET_CONTENT));
                                            }
                                            turl.similarity = similarity;
                                            return turl;
                                        }
                                    });
                                    urldownloadSubmissionCount++;
                                }
                            }
                            // to make sure all qtp pairs have the same feature number
                            searchresults.get(tweetid).updateFeatures(Configuration.TWEET_URL_TITLE, 0);
                        }
                        if (!Double.isNaN(hit.score)) {
                            searchresults.get(tweetid).updateFeatures(QueryTweetPair.concatModelQuerytypeFeature(model, querytype), hit.score);
                        } else {
                            logger.error("lucene returned score: " + hit.score);
                        }
                    }
                }
            }
            /**
             * pick up the urltitle-query similarity score from completion
             * service
             */
            long start = System.currentTimeMillis();
            try {
                for (int t = 0; t < urldownloadSubmissionCount; t++) {
                    Future<TweetidUrl> f = urldownloader.take();
                    TweetidUrl turl = f.get();
                    if (turl != null) {
                        long tid = turl.tweetid;
                        double similarity = turl.similarity;
                        if (Double.isNaN(similarity)) {
                            logger.error("lucene returned score: " + similarity);
                            continue;
                        }
                        if (similarity >= 0) {
                            searchresults.get(tid).updateFeatures(Configuration.TWEET_URL_TITLE, similarity);
                            searchresults.get(tid).setURLTileString(turl.urltitle);
                        } else {
                            logger.error("similarity is " + similarity);
                        }
                    } else {
                        logger.error("turl is " + turl);
                    }
                }
            } catch (Exception ex) {
                logger.error("", ex);
            }
            long end = System.currentTimeMillis();
            logger.info("download url took: " + (end - start) + " for " + urldownloadSubmissionCount);

            /**
             * conduct pointwise prediction
             */
            for (QueryTweetPair qtp : searchresults.valueCollection()) {
                pwScorer.predictor(qtp);
            }

            UniqQuerySearchResult uqsr = new UniqQuerySearchResult(queryid, searchresults.valueCollection());
            return uqsr;
        }

        @Override
        public UniqQuerySearchResult call() throws Exception {
            UniqQuerySearchResult qtpairs = mutliScorers(this.reader, this.querytypeQuery, this.queryid, this.topN);
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

        private void offer2queue(BlockingQueue<QueryTweetPair> queue2offer4LW) throws InterruptedException {
            for (QueryTweetPair qtp : results) {
                // offer to the blocking queue for the decision maker
                boolean isSucceed = queue2offer4LW.offer(new QueryTweetPair(qtp), 1000, TimeUnit.MILLISECONDS);
                if (!isSucceed) {
                    logger.error("offer to queue2offer4LW failed: " + queue2offer4LW.size());
                }
            }
        }

        private void offer2queue(BlockingQueue<QueryTweetPair> queue2offer4PW, BlockingQueue<QueryTweetPair> queue2offer4LW) throws InterruptedException {
            for (QueryTweetPair qtp : results) {
                // offer to the blocking queue for the pointwise decision maker
                boolean isSucceed = queue2offer4PW.offer(new QueryTweetPair(qtp), 1000, TimeUnit.MILLISECONDS);
                if (!isSucceed) {
                    logger.error("offer to queue2offer4PW failed: " + queue2offer4PW.size());
                }
                // offer to the blocking queue for the decision maker
                isSucceed = queue2offer4LW.offer(new QueryTweetPair(qtp), 1000, TimeUnit.MILLISECONDS);
                if (!isSucceed) {
                    logger.error("offer to queue2offer4LW failed: " + queue2offer4LW.size());
                }
            }
        }

    }

}
