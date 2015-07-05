package de.mpii.microblogtrack.utility;

/**
 *
 * @author khui
 */
public class MYConstants {

    public final static String RUN_ID = "myTest";

    /**
     * fields name used for retrieval of tweets
     */
    public final static String TWEET_ID = "tweetid";
    // count of tweets being downloaded
    public final static String TWEET_COUNT = "tweetcountid";

    public final static String TWEET_CONTENT = "tweetcontent";

    public final static String TWEET_URLCONTENT = "langdingpage";

    public final static String TWEET_CONTENT_URLEXPEND = "tweeturl";
    /**
     * fields name for query
     */
    public final static String QUERY_ID = "queryId";

    public final static String QUERY_STR = "query";

    public final static String QUERY_DESC = "description";

    public final static String QUERY_NARR = "narrative";
    /**
     * additional field names for result printer
     */
    public final static String RES_RANK = "rank";

    public final static String RES_RUNINFO = "runinfo";
    /**
     * parameters for lucene systems
     */
    // the size of the in-memory index, determining how often the writer dump the index to the disk
    public final static double LUCENE_MEM_SIZE = 1024.0 * 10;

    public final static String LUCENE_TOKENIZER = "org.apache.lucene.analysis.en.EnglishAnalyzer";
    // lucene perform search per LUCENE_SEARCH_FREQUENCY seconds
    public final static int LUCENE_SEARCH_FREQUENCY = 60;
    // every time invertal, we retrieve top_n tweets for each query for further processin from lucene
    public final static int LUCENE_TOP_N_SEARCH = 30;
    // number of threads we use to conduct multiquery search
    public final static int LUCENE_SEARCH_THREADNUM = 10;
    /**
     * decision maker parameters
     */
    // adjust the threshold for pop-up tweets dynamically, governing the adjusting step
    public final static double DECISION_MAKER_THRESHOLD_ALPHA = 0.005;
    // filter out the tweet with less than threshold relative score
    public final static double DECISION_MAKER_SCORE_FILTER = 0.95;
    // if it is the first tweet to pop-up, we require a relative high threshold
    public final static double DECISION_MAKER_FIRSTPOPUP_SCORETHRESD = 0.999;
    // filter out the tweets that are too similar with at least one of the pop-up tweet, the number is relative to 
    // average distance among centroids
    public final static double DECISION_MAKER_DIST_FILTER = 0.2;
    // start delay for the decision maker in minutes 
    public final static int DECISION_MAKER_START_DELAY = 15;
    // decision maker calling period in minutes, should be 1440 if one day is a period  
    public final static int DECISION_MAKER_PERIOD = 30;
    /**
     * tweet tracker parameters: track both relative score and tweet clustering
     * centroids
     */
    // parameter required in ball kmean algorithm, the maximum times of iteration
    public final static int TRACKER_BALLKMEAN_MAX_ITERATE = 3;
    // the computation for the upper bound of cluster numbers is computed as #desired cluster * log(#data number)
    // we estimate the expected tweet number for 10 hours
    public final static int TRACKER_SKMEAN_CLUSTERNUM_UPBOUND = 500;
    // the clustering distance measure for the clustering algorithm
    // SquaredEuclideanDistanceMeasure, CosineDistanceMeasure, EuclideanDistanceMeasure etc..
    public final static String TRACKER_DISTANT_MEASURE = "org.apache.mahout.common.distance.CosineDistanceMeasure";
    // every x minutes, we update the average distance among centroids, by re-clustering the existing centroids
    // from streaming k-mean, which is relative expensive
    public final static double TRACKER_AVGDIST_UPDATE_MINUTES = 30;
    // when we convert the absolute pointwise predict score to relative score,
    // we only convert the top-p% for effiency reason
    public final static double TRACKER_CUMULATIVE_TOPPERC = 0.5;
    // how accurate we compute the cumulative probability in converting the absolute predicting score
    // by governing how many digits we want to retain, i.e., the number of bins we have
    public final static int TRACKER_CUMULATIVE_GRANULARITY = DECISION_MAKER_START_DELAY * LUCENE_TOP_N_SEARCH;
    /**
     * pointwise predictor outcome: confidence, score, etc..
     */
    public final static String PRED_ABSOLUTESCORE = "absolutePrimaryScore";

    public final static String PRED_RELATIVESCORE = "relativePrimaryScore";
    /**
     * feature names
     */
    public final static String FEATURE_BM25 = "bm25";

    public final static String FEATURE_TFIDF = "tfidf";

    public final static String FEATURE_LMD = "lmDirichlet";

    public final static String FEATURE_RETWEETNUM = "retweet_num";

    public final static String FEATURE_LIKENUM = "like_num";

    public final static String FEATURE_HASHTAG = "hashtag";

    public final static String FEATURE_URL = "tweeturl";

    public final static String[] FEATURES_SEMANTIC = new String[]{FEATURE_TFIDF, FEATURE_BM25, FEATURE_LMD};

    public final static String[] FEATURES_EXPANSION = new String[]{};

    public final static String[] FEATURES_TWEETQUALITY = new String[]{FEATURE_RETWEETNUM, FEATURE_LIKENUM, FEATURE_HASHTAG, FEATURE_URL};

    public final static String[] FEATURES_USERAUTHORITY = new String[]{};

    /**
     * other parameters
     */
    // number of thread we use to listen the api, 1 is enough
    public final static int LISTENER_THREADNUM = 1;
}
