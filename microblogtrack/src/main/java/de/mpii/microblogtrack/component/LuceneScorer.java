package de.mpii.microblogtrack.component;

import de.mpii.microblogtrack.component.filter.Filter;
import de.mpii.microblogtrack.component.filter.LangFilterTW;
import de.mpii.microblogtrack.component.predictor.PointwiseScorer;
import de.mpii.microblogtrack.userprofiles.TrecQuery;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import gnu.trove.map.TLongObjectMap;
import de.mpii.microblogtrack.utility.MYConstants;
import de.mpii.microblogtrack.utility.ResultTweetsTracker;
import de.mpii.microblogtrack.utility.ResultTrackerKMean;
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

    private final Map<String, ResultTweetsTracker> queryResultTrackers;

    private static final String[] searchModels = MYConstants.irModels;

    private final PointwiseScorer pwScorer;

    public LuceneScorer(String indexdir, Map<String, ResultTweetsTracker> queryTweetList, PointwiseScorer pwScorer) throws IOException {
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
        this.queryResultTrackers = queryTweetList;
        this.pwScorer = pwScorer;
    }

    public void multiQuerySearch(String queryfile, BlockingQueue<QueryTweetPair> tweetqueue) throws IOException, InterruptedException, ExecutionException, ParseException {
        TrecQuery tq = new TrecQuery();
        Map<String, Query> queries = tq.readInQueries(queryfile, this.analyzer, MYConstants.TWEETSTR);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        final ScheduledFuture<?> sercherHandler = scheduler.scheduleAtFixedRate(new MultiQuerySearcher(queries, tweetqueue), 60, 60, TimeUnit.SECONDS);
        // the task will be canceled after running 30 days automatically
        scheduler.schedule(() -> {
            sercherHandler.cancel(true);
        }, 30, TimeUnit.DAYS);
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
        Query termquery = qb.createBooleanQuery(MYConstants.TWEETSTR, "RT");
        Query phrasequery = qb.createPhraseQuery(MYConstants.TWEETSTR, "thanks for");
        long[] minmax = indexTracker.getAcurateTweetCount();
        rangeQuery = NumericRangeQuery.newLongRange(MYConstants.TWEETNUM, minmax[0], minmax[1], true, false);
        BooleanQuery combinedQuery = new BooleanQuery();
        combinedQuery.add(rangeQuery, BooleanClause.Occur.MUST);
        combinedQuery.add(termquery, BooleanClause.Occur.SHOULD);
        combinedQuery.add(phrasequery, BooleanClause.Occur.SHOULD);
        logger.info("***************************************************************************");
        logger.info("***************************************************************************");
        logger.info("indexed documents: " + minmax[0] + " -------- " + (minmax[1] - 1));
        if (directoryReader != null) {
            qtpairs = null;
            //mutliScorers(directoryReader, combinedQuery, MYConstants.QUERYID, 10);
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
                doc.add(new LongField(MYConstants.TWEETNUM, tweetcountId, Field.Store.YES));
                doc.add(new LongField(MYConstants.TWEETID, tweet.getId(), Field.Store.YES));
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
        fieldnameStr.put(MYConstants.TWEETSTR, str);
        return fieldnameStr;

    }

    private class MultiQuerySearcher implements Runnable {

        private final Map<String, Query> queries;

        private int threadnum = MYConstants.MULTIQUERYSEARCH_THREADNUM;

        private final BlockingQueue<QueryTweetPair> tweetqueue;

        public MultiQuerySearcher(final Map<String, Query> queries, BlockingQueue<QueryTweetPair> tweetqueue) {
            this.queries = queries;
            this.tweetqueue = tweetqueue;
        }

        public void setThreadNum(int threadnum) {
            this.threadnum = threadnum;
        }

        @Override
        public void run() {
            BooleanQuery combinedQuery;
            Executor excutor = Executors.newFixedThreadPool(threadnum);
            CompletionService<Collection<QueryTweetPair>> completeservice = new ExecutorCompletionService<>(excutor);
            long[] minmax = indexTracker.getAcurateTweetCount();
            NumericRangeQuery rangeQuery = NumericRangeQuery.newLongRange(MYConstants.TWEETNUM, minmax[0], minmax[1], true, false);
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
                NumericRangeQuery boundaryTest = NumericRangeQuery.newLongRange(MYConstants.TWEETNUM, minmax[1] - 1, minmax[1] - 1, true, true);
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
                logger.info(resultnum);
                Collection<QueryTweetPair> queryranking;
                for (int i = 0; i < resultnum; ++i) {
                    try {
                        queryranking = completeservice.take().get();
                        if (queryranking != null) {
                            for (QueryTweetPair qtp : queryranking) {
                                // update the result tracker
                                queryResultTrackers.get(qtp.queryid).addTweet(qtp);
                                // offer to the blocking queue for the decision maker
                                boolean isSucceed = tweetqueue.offer(new QueryTweetPair(qtp), 100, TimeUnit.MILLISECONDS);
                                logger.info(qtp.toString());
                                if (!isSucceed) {
                                    logger.error("offer to queue failed.");
                                }
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

    private class UniqQuerySearcher implements Callable<Collection<QueryTweetPair>> {

        private int topN = MYConstants.TOP_N_FROM_LUCENE;

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

        private Collection<QueryTweetPair> mutliScorers(IndexReader reader, Query query, String queryId, int topN) throws IOException {
            TLongObjectMap<QueryTweetPair> searchresults = new TLongObjectHashMap<>();
            IndexSearcher searcherInUse;
            ScoreDoc[] hits;
            Document tweet;
            long tweetid;
            for (String name : searchModels) {
                searcherInUse = new IndexSearcher(reader);
                switch (name) {
                    case MYConstants.TFIDF:
                        break;
                    case MYConstants.BM25:
                        searcherInUse.setSimilarity(new BM25Similarity());
                        break;
                    case MYConstants.LMD:
                        searcherInUse.setSimilarity(new LMDirichletSimilarity());
                        break;
                }
                hits = searcherInUse.search(query, topN).scoreDocs;
                for (ScoreDoc hit : hits) {
                    tweet = searcherInUse.doc(hit.doc);
                    tweetid = Long.parseLong(tweet.get(MYConstants.TWEETID));
                    if (!searchresults.containsKey(tweetid)) {
                        searchresults.put(tweetid, new QueryTweetPair(tweetid, queryId, indexTracker.getStatus(tweetid)));
                    }
                    searchresults.get(tweetid).updateFeatures(name, hit.score);
                }
            }
            /**
             * conduct pointwise prediction
             */
            for (QueryTweetPair qtp : searchresults.valueCollection()) {
                pwScorer.predictor(qtp);
            }

            return searchresults.valueCollection();
        }

        @Override
        public Collection<QueryTweetPair> call() throws Exception {
            Collection<QueryTweetPair> qtpairs = mutliScorers(this.reader, this.query, this.queryid, this.topN);
            return qtpairs;
        }
    }

}
