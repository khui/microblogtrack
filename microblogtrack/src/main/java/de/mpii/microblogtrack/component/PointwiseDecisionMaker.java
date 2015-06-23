package de.mpii.microblogtrack.component;

import de.mpii.microblogtrack.utility.CandidateTweet;
import de.mpii.microblogtrack.utility.QueryTweets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.log4j.Logger;

/**
 * periodically run to pick up tweet for notification
 *
 * @author khui
 */
public class PointwiseDecisionMaker implements Callable<Void> {

    static Logger logger = Logger.getLogger(PointwiseDecisionMaker.class);

    private final Map<String, QueryTweets> queryTweetList;

    private final int centroidnum = 10;
    // the number of closest tweets w.r.t. each centroid being selected
    private final int topk = 3;

    // track the tweets being sent
    private final Map<String, CandidateTweet> qidTweetSent = new HashMap<>();

    public PointwiseDecisionMaker(Map<String, QueryTweets> queryTweetList) {
        this.queryTweetList = queryTweetList;
    }

    @Override
    public Void call() throws Exception {
        for (String queryid : queryTweetList.keySet()) {
            decisionMake(queryid, queryTweetList.get(queryid).getToptweetsEachCentroid(centroidnum, topk));
        }
        return null;
    }

    /**
     * "send the tweet" and keep tracking in qidTweetSent
     *
     * @param candidatetweets
     */
    private void decisionMake(String qid, List<CandidateTweet> candidatetweets) {
        //qidTweetSent
    }

}
