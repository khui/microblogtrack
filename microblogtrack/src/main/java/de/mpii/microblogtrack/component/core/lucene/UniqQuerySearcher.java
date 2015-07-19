package de.mpii.microblogtrack.component.core.lucene;

import de.mpii.microblogtrack.component.ExtractTweetText;
import de.mpii.microblogtrack.component.IndexTracker;
import de.mpii.microblogtrack.component.TweetStringSimilarity;
import de.mpii.microblogtrack.component.predictor.PointwiseScorer;
import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
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
import twitter4j.URLEntity;

/**
 *
 * @author khui
 */
public class UniqQuerySearcher implements Callable<UniqQuerySearchResult> {

    static Logger logger = Logger.getLogger(UniqQuerySearcher.class.getName());

    private final int topN = Configuration.LUCENE_TOP_N_SEARCH;

    private final String queryid;

    private final Map<String, Query> querytypeQuery;

    private final DirectoryReader reader;

    private final Executor downloadURLExcutor;

    // track duplicate tweet and allocate unique tweetCountId to each received tweet
    private final IndexTracker indexTracker;

    private final ExtractTweetText textextractor;

    private final PointwiseScorer pwScorer;

    public UniqQuerySearcher(Map<String, Query> querytypeQuery, String queryId,
            DirectoryReader reader, Executor downloadURLExcutor, IndexTracker indexTracker,
            ExtractTweetText textextractor,
            PointwiseScorer pwScorer) {
        this.querytypeQuery = querytypeQuery;
        this.queryid = queryId;
        this.reader = reader;
        this.downloadURLExcutor = downloadURLExcutor;
        this.indexTracker = indexTracker;
        this.textextractor = textextractor;
        this.pwScorer = pwScorer;
    }

    private UniqQuerySearchResult mutliScorers(IndexReader reader, Map<String, Query> querytypeQuery, String queryId) throws Exception {
        CompletionService<ExtractTweetText.TweetidUrl> urldownloader = new ExecutorCompletionService<>(downloadURLExcutor);
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
                        /**
                         * the url downloader can be shut down by pass
                         * textextractor as null
                         */
                        if (textextractor != null) {
                            URLEntity[] urlentity = indexTracker.getStatus(tweetid).getURLEntities();
                            if (urlentity != null) {
                                if (urlentity.length > 0) {
                                    String url = urlentity[0].getURL();
                                    final ExtractTweetText.TweetidUrl turl = textextractor.new TweetidUrl(tweetid, url);
                                    urldownloader.submit(new Callable() {

                                        @Override
                                        public ExtractTweetText.TweetidUrl call() throws Exception {
                                            double similarity = 0;
                                            textextractor.getUrlTitle(turl);
                                            if (turl.isAvailable) {
                                                similarity = TweetStringSimilarity.strJarcard(turl.urltitle,
                                                        querytypeQuery.get(Configuration.QUERY_TITLE).toString(Configuration.TWEET_CONTENT));

                                                //logger.info(similarity + "\t" + querytypeQuery.get(Configuration.QUERY_TITLE).toString(Configuration.TWEET_CONTENT) + "\t" + turl.urltitle);
                                            }
                                            turl.similarity = similarity;
                                            return turl;
                                        }
                                    });
                                    urldownloadSubmissionCount++;
                                }
                            }
                        }
                        // to make sure all qtp pairs have the same feature number
                        // searchresults.get(tweetid).updateFeatures(Configuration.TWEET_URL_TITLE, 0);
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
         * pick up the urltitle-query similarity score from completion service
         */
        // long start = System.currentTimeMillis();
        if (textextractor != null) {
            try {
                for (int t = 0; t < urldownloadSubmissionCount; t++) {
                    Future<ExtractTweetText.TweetidUrl> f = urldownloader.take();
                    ExtractTweetText.TweetidUrl turl = f.get(Configuration.LUCENE_DOWNLOAD_URL_TIMEOUT, TimeUnit.MILLISECONDS);
                    if (turl != null) {
                        long tid = turl.tweetid;
                        double similarity = turl.similarity;
                        if (Double.isNaN(similarity)) {
                            logger.error("urltitle similarity score: " + similarity);
                            continue;
                        }
                        if (similarity >= 0) {
                            searchresults.get(tid).updateFeatures(Configuration.TWEET_URL_TITLE, similarity);
                            searchresults.get(tid).setURLTitle(turl.urltitle);
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
        }
        //  long end = System.currentTimeMillis();
        //   logger.info("download url took: " + (end - start) + " for " + urldownloadSubmissionCount);

        /**
         * conduct pointwise prediction
         */
        if (pwScorer != null) {
            for (QueryTweetPair qtp : searchresults.valueCollection()) {
                pwScorer.predictor(qtp);
                //logger.info(qtp.getAbsScore() + "\t" + qtp.queryid + "\t" + qtp.getTweetText());
            }
        }
        UniqQuerySearchResult uqsr = new UniqQuerySearchResult(queryid, searchresults.valueCollection());
        return uqsr;
    }

    @Override
    public UniqQuerySearchResult call() throws Exception {
        UniqQuerySearchResult qtpairs = mutliScorers(this.reader, this.querytypeQuery, this.queryid);
        return qtpairs;
    }

}
