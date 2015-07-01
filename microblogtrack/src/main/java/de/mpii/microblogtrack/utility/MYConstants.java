package de.mpii.microblogtrack.utility;

/**
 *
 * @author khui
 */
public class MYConstants {

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
    public final static int MULTIQUERYSEARCH_THREADNUM = 12;

    public final static int LISTENER_THREADNUM = 2;

    public final static int TOP_N_FROM_LUCENE = 100;

    public final static int MAX_ITERATE_BALLKMEAN = 3;
    // compute how many latest tweets we want to retain for each query: TOP_N_FROM_LUCENE * RECORD_MINIUTES
    public final static int RECORD_MINIUTES = 20;
    // the computation for the upper bound of cluster numbers is computed as #desired cluster * log(#data number)
    // we estimate the expected tweet number for 10 hours
    public final static int STREAMKMEAN_CLUSTERNUM = 400;
    // the clustering distance measure for the clustering algorithm
    // SquaredEuclideanDistanceMeasure, CosineDistanceMeasure, EuclideanDistanceMeasure etc..
    public final static String DISTANT_MEASURE_CLUSTER = "org.apache.mahout.common.distance.CosineDistanceMeasure";

    public final static double DECISION_MAKER_THRESHOLD_ALPHA = 0.05;

    public final static double DECISION_MAKER_SCORE_FILTER = 0.95;

    public final static double DECISION_MAKER_FIRSTPOPUP_SCORETHRESD = 0.99;

    public final static double DECISION_MAKER_DIST_FILTER = 0.2;
    // every x minutes, we update the average distance among centroids, by re-clustering the existing centroids
    // from streaming k-mean
    public final static double TRACKER_AVGDIST_UPDATE_MINUTES = 2;
    // when we convert the absolute pointwise predict score to relative score,
    // we only convert the top-p% for effiency reason
    public final static double TRACKER_CUMULATIVE_TOPPERC = 0.5;
    // how accurate we compute the cumulative probability in converting the absolute score
    // govern how many digits we retain in maping the absolute value to the bin
    public final static int TRACKER_CUMULATIVE_GRANULARITY = 100;

    /**
     * pointwise predictor outcome: confidence, score, etc..
     */
    public final static String PRED_ABSOLUTESCORE = "absolutePrimaryScore";

    public final static String PRED_RELATIVESCORE = "relativePrimaryScore";

}
