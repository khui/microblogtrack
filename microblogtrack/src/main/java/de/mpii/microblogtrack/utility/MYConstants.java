package de.mpii.microblogtrack.utility;

/**
 *
 * @author khui
 */
public class MYConstants {

    public final static String RUNSTRING = "myTest";

    /**
     * retrieval model used for feature
     */
    public final static String BM25 = "bm25";

    public final static String TFIDF = "tfidf";

    public final static String LMD = "lmDirichlet";

    public final static String[] irModels = new String[]{TFIDF, BM25, LMD};
    /**
     * fields name used for retrieval of tweets
     */
    public final static String TWEETID = "tweetid";
    // count of tweets being downloaded
    public final static String TWEETNUM = "tweetcountid";

    public final static String TWEETSTR = "tweetcontent";

    public final static String URLSTR = "langdingpage";

    public final static String TWEETURL = "tweeturl";
    /**
     * fields name for query
     */
    public final static String QUERYID = "queryId";

    public final static String QUERYSTR = "query";

    public final static String DESCRIPTION = "description";

    public final static String NARRATIVE = "narrative";
    /**
     * default parameters settings
     */
    // number of threads we use to conduct multiquery search
    public final static int MULTIQUERYSEARCH_THREADNUM = 10;
    // number of thread we use to listen the api, 1 is enough
    public final static int LISTENER_THREADNUM = 1;
    // every time invertal, we retrieve top_n tweets for each query for further processin from lucene
    public final static int TOP_N_FROM_LUCENE = 30;
    // parameter required in ball kmean algorithm, the maximum times of iteration
    public final static int MAX_ITERATE_BALLKMEAN = 3;
    // lucene perform search per LUCENE_SEARCH_FREQUENCY seconds
    public final static int LUCENE_SEARCH_FREQUENCY = 60;
    // the computation for the upper bound of cluster numbers is computed as #desired cluster * log(#data number)
    // we estimate the expected tweet number for 10 hours
    public final static int STREAMKMEAN_CLUSTERNUM = 500;
    // the clustering distance measure for the clustering algorithm
    // SquaredEuclideanDistanceMeasure, CosineDistanceMeasure, EuclideanDistanceMeasure etc..
    public final static String DISTANT_MEASURE_CLUSTER = "org.apache.mahout.common.distance.CosineDistanceMeasure";
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
    // every x minutes, we update the average distance among centroids, by re-clustering the existing centroids
    // from streaming k-mean, which is relative expensive
    public final static double TRACKER_AVGDIST_UPDATE_MINUTES = 30;
    // when we convert the absolute pointwise predict score to relative score,
    // we only convert the top-p% for effiency reason
    public final static double TRACKER_CUMULATIVE_TOPPERC = 0.5;
    // how accurate we compute the cumulative probability in converting the absolute predicting score
    // by governing how many digits we want to retain, i.e., the number of bins we have
    public final static int TRACKER_CUMULATIVE_GRANULARITY = DECISION_MAKER_START_DELAY * TOP_N_FROM_LUCENE;
    /**
     * parameters for lucene systems
     */
    // the size of the in-memory index, determining how often the writer dump the index to the disk
    public final static double LUCENE_MEM_SIZE = 1024.0 * 10;

    /**
     * pointwise predictor outcome: confidence, score, etc..
     */
    public final static String PRED_ABSOLUTESCORE = "absolutePrimaryScore";

    public final static String PRED_RELATIVESCORE = "relativePrimaryScore";

    /**
     * additional field names for result printer
     */
    public final static String RES_RANK = "rank";

    public final static String RES_RUNINFO = "runinfo";

}
