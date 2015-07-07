package de.mpii.microblogtrack.utility;

import java.util.Arrays;
import java.util.List;

/**
 *
 * @author khui
 */
public class Configuration {

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
     * pointwise decision maker parameters for mobile notification task
     */
    // adjust the threshold for pop-up tweets dynamically, governing the adjusting step
    public final static double PW_DM_THRESHOLD_ALPHA = 0.005;
    // filter out the tweet with less than threshold relative score
    public final static double PW_DM_SCORE_FILTER = 0.95;
    // if it is the first tweet to pop-up, we require a relative high threshold
    public final static double PW_DM_FIRSTPOPUP_SCORETHRESD = 0.999;
    // filter out the tweets that are too similar with at least one of the pop-up tweet, the number is relative to 
    // average distance among centroids
    public final static double PW_DM_DIST_FILTER = 0.2;
    // start delay for the decision maker in minutes 
    public final static int PW_DM_START_DELAY = 15;
    // decision maker calling period in minutes, should be 1440 if one day is a period  
    public final static int PW_DM_PERIOD = 30;
    /**
     * listwise decision maker for e-mail digest task
     */
    // start delay for the listwise decision maker
    public final static int LW_DM_START_DELAY = 15;
    // the length of the priority queue: tracking at most n tweets with highest pointwise prediction score
    public final static int LW_DM_QUEUE_LEN = 2000;

    public final static int LW_DM_SELECTNUM = 100;

    /**
     * parameter for maxrep
     */
    public final static double MAXREP_SIMI_THRESHOLD = 0.8;

    public final static String MAXREP_DISTANT_MEASURE = "org.apache.mahout.common.distance.CosineDistanceMeasure";
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
    public final static int TRACKER_CUMULATIVE_GRANULARITY = PW_DM_START_DELAY * LUCENE_TOP_N_SEARCH;
    /**
     * pointwise predictor outcome: confidence, score, etc..
     */
    public final static String PRED_ABSOLUTESCORE = "absolutePrimaryScore";

    public final static String PRED_RELATIVESCORE = "relativePrimaryScore";
    /**
     * feature names
     */
    public final static String FEATURE_S_BM25 = "bm25";

    public final static String FEATURE_S_TFIDF = "tfidf";

    public final static String FEATURE_S_LMD = "lmDirichlet";

    public final static String FEATURE_S_LMJM = "lmJelinekMercer";

    public final static float FEATURE_S_LMJM_Lambda = 0.8f;

    public final static String FEATURE_T_RETWEETNUM = "tweet_retweet_count";

    public final static String FEATURE_T_FAVORITENUM = "tweet_favorite_count";

    public final static String FEATURE_T_HASHTAGNUM = "tweet_hashtag_count";

    public final static String FEATURE_T_URLNUM = "tweet_url_count";

    public final static String FEATURE_T_USERMENTIONNUM = "tweet_usermention_count";

    public final static String FEATURE_T_MEDIANUM = "tweet_media_count";

    public final static String FEATURE_U_FOLLOWERNUM = "user_follower_count";

    public final static String FEATURE_U_DESC_LEN = "user_description_length";

    public final static String FEATURE_U_DESC_URLNUM = "user_descriptionurl_count";

    public final static String FEATURE_U_FAVORITENUM = "user_favorite_count";

    public final static String FEATURE_U_FRIENDNUM = "user_friend_count";

    public final static String FEATURE_U_LISTNUM = "user_inlist_count";

    public final static String FEATURE_U_STATUSNUM = "user_status_count";

    public final static String FEATURE_U_ISCELEBRITY = "user_iscelebrity";

    public final static String FEATURE_U_ISDEFAULT_ICON = "user_isdefault_profileimage";
    // feature name array to categorize above features into four classes
    public final static String[] FEATURES_SEMANTIC = new String[]{FEATURE_S_TFIDF, FEATURE_S_BM25, FEATURE_S_LMD, FEATURE_S_LMJM};

    public final static String[] FEATURES_EXPANSION = new String[]{};

    public final static String[] FEATURES_TWEETQUALITY = new String[]{FEATURE_T_RETWEETNUM, FEATURE_T_FAVORITENUM, FEATURE_T_HASHTAGNUM, FEATURE_T_URLNUM, FEATURE_T_USERMENTIONNUM, FEATURE_T_MEDIANUM};

    public final static String[] FEATURES_USERAUTHORITY = new String[]{FEATURE_U_FOLLOWERNUM, FEATURE_U_DESC_LEN, FEATURE_U_DESC_URLNUM, FEATURE_U_FAVORITENUM, FEATURE_U_FRIENDNUM, FEATURE_U_LISTNUM, FEATURE_U_STATUSNUM, FEATURE_U_ISCELEBRITY, FEATURE_U_ISDEFAULT_ICON};
    // for features that only have binary value, should not be scaled in feature normalization
    public final static List<String> FEATURES_NO_SCALE = Arrays.asList(new String[]{FEATURE_U_ISCELEBRITY, FEATURE_U_ISDEFAULT_ICON});
    /**
     * other parameters
     */
    // number of thread we use to listen the api, 1 is enough
    public final static int LISTENER_THREADNUM = 1;
}
