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

    public final static int TOP_N_FROM_LUCENE = 3;
    
    public final static int MAX_ITERATE_BALLKMEAN = 3;
    // compute how many latest tweets we want to retain for each query: TOP_N_FROM_LUCENE * RECORD_MINIUTES
    public final static int RECORD_MINIUTES = 20;
    /**
     * pointwise predictor outcome: confidence, score, etc..
     */
    public final static String PREDICTSCORE = "predictScore";

}
