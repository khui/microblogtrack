package de.mpii.microblogtrack.component;

import de.mpii.microblogtrack.utility.CandidateTweet;
import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import de.mpii.microblogtrack.utility.io.printresult.ResultPrinter;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import org.apache.log4j.Logger;

/**
 * periodically run to pick up tweet for notification
 *
 * @author khui
 */
public class PointwiseDecisionMaker extends SentTweetTracker implements Runnable {

    static Logger logger = Logger.getLogger(PointwiseDecisionMaker.class);

    private final TObjectDoubleMap<String> queryidThresholds = new TObjectDoubleHashMap<>(250);
    // dynamic threshold for the initial tweet, avoiding the most relevant tweets never come
    private final TObjectDoubleMap<String> queryidInitThresholds = new TObjectDoubleHashMap<>(250);

    private final BlockingQueue<QueryTweetPair> tweetqueue;

    private final TObjectIntMap<String> queryNumberCount = new TObjectIntHashMap<>(250);

    private final int centroidnum = 10;

    private final Set<String> finishedQueryId = new HashSet<>(250);

    private final ResultPrinter resultprinter;

    public PointwiseDecisionMaker(Map<String, ResultTweetsTracker> tracker, BlockingQueue<QueryTweetPair> tweetqueue, ResultPrinter resultprinter) throws ClassNotFoundException, InstantiationException, IllegalAccessException, FileNotFoundException {
        super(tracker);
        this.tweetqueue = tweetqueue;
        this.resultprinter = resultprinter;
        for (String qid : tracker.keySet()) {
            queryNumberCount.put(qid, 1);
        }
    }

    @Override
    public void run() {
        for (String qid : queryResultTrackers.keySet()) {
            queryResultTrackers.get(qid).informStart2Record();
        }
        QueryTweetPair tweet;
        String queryid;
        /**
         * check the interrupt flag in each loop, since this is how we terminate
         * the decision maker from the timer if after one day, there still exist
         * query has less than 10 results.
         */
        while (true) {
            if (Thread.interrupted()) {
                clear();
                logger.info("current PointwiseDecisionMaker has been interrupted");
                break;
            }
            tweet = tweetqueue.poll();
            if (tweet == null) {
                continue;
            }
            if (centroidnum > queryNumberCount.get(tweet.queryid)) {
                queryid = tweet.queryid;
                if (scoreFilter(tweet)) {
                    double[] distances = distFilter(tweet);
                    if (distances != null) {
                        CandidateTweet resultTweet = decisionMake(tweet, distances);
                        if (resultTweet.rank > 0) {
                            try {
                                // write down the tweets that are notified
                                resultprinter.println(queryid, resultTweet.forDebugToString(tweet.getStatus().getText()));
                            } catch (FileNotFoundException ex) {
                                logger.error("", ex);
                            }
                            queryNumberCount.adjustValue(queryid, 1);

                            //logger.info(queryNumberCount.get(queryid) + " " + resultTweet.toString() + " " + tweet.getStatus().getText() + " " + tweetqueue.size());
                        } else {
                            //logger.info("tweet has not been selected: " + tweet.getRelScore() + "  " + tweet.getAbsScore() + " " + queryidThresholds.get(tweet.queryid));
                        }
                    } else {
                        //logger.info("filter out the tweet by distance: " + qidTweetSent.get(tweet.queryid).size());
                    }
                }
            } else {
                logger.info("Finished: " + tweet.queryid + " " + queryNumberCount.get(tweet.queryid));
                finishedQueryId.add(tweet.queryid);
                if (finishedQueryId.size() >= queryNumberCount.size()) {
                    logger.info("Finished all! " + finishedQueryId.size());
                    clear();
                    break;
                }
            }
        }

    }

    private void clear() {
        resultprinter.flush();
    }

    /**
     * filter out the tweets that with low score
     *
     * @param tweet
     * @return
     */
    private boolean scoreFilter(QueryTweetPair tweet) {
        boolean isRetain = false;
        double relativeScore = tweet.getRelScore();
        if (relativeScore > Configuration.PW_DM_SCORE_FILTER) {
            isRetain = true;
        }
        return isRetain;
    }

    /**
     * "send the tweet" and keep tracking all the pop-up tweets in qidTweetSent
     * meanwhile adjusting/maintaining the threshold for the gain to make
     * decision. Intuitively, a tweet will be pop-up as long as it is highly
     * relevant and divergent enough from previously notification. In this
     * decision maker, the gain is computed as the sum-product of the absolute
     * relevance and the relative distance. The absolute relevance is from the
     * predictor, e.g., the output probability for the point belonging to class
     * 1. And the relative distance is the distance between current tweets and
     * notified tweets w.r.t. the average distance among current centroids. All
     * other decision makers should override this method.
     *
     * @param tweet
     * @param relativeDist
     * @return
     */
    protected CandidateTweet decisionMake(QueryTweetPair tweet, double[] relativeDist) {
        double absoluteScore = tweet.getAbsScore();
        double relativeScore = tweet.getRelScore();
        String queryId = tweet.queryid;

        double avggain = 0;
        CandidateTweet resultTweet = new CandidateTweet(tweet.tweetid, absoluteScore, relativeScore, queryId, tweet.vectorizeMahout());
        // the distances w.r.t. all popped up tweets
        if (relativeDist.length > 0) {
            for (double dist : relativeDist) {
                avggain += dist * absoluteScore;
            }
            avggain /= (double) relativeDist.length;
            // if the average gain of the current tweet were larger than
            // the threshold, then pop-up the current tweet and lift the threshold
            // otherwise decrease the threshold
            if (adjustThreshold(queryId, avggain)) {
                resultTweet.rank = queryNumberCount.get(queryId);
            }
        } else {
            // if this is the first tweet to pop-up, we should make sure this is the 
            // tweet that have nearly highest relevance score, meanwhile we store this
            // relevance as threshold
            if (!queryidInitThresholds.containsKey(queryId)) {
                queryidInitThresholds.put(queryId, Configuration.PW_DM_FIRSTPOPUP_SCORETHRESD);
            }
            double currentThread = queryidInitThresholds.get(queryId);
            if (relativeScore > currentThread) {
                avggain = absoluteScore;
                resultTweet.rank = queryNumberCount.get(queryId);
                adjustThreshold(queryId, avggain);
            } else {
                currentThread *= (1 - Configuration.PW_DM_THRESHOLD_ALPHA);
                queryidInitThresholds.put(queryId, currentThread);
            }
        }
        // keep tracking all tweets being pop-up in the qidTweetSent
        if (resultTweet.rank > 0) {
            if (!qidTweetSent.containsKey(queryId)) {
                qidTweetSent.put(queryId, new ArrayList<>());
            }
            qidTweetSent.get(queryId).add(new CandidateTweet(resultTweet));
        }
        return resultTweet;
    }

    /**
     * return true or false according to the comparison between current gain and
     * the existing threshold. If currentGain >= threshold, return true and lift
     * the threshold; if currentGain < threshold, return false and decrease the
     * threshold; it this is the first tweet for this query, i.e., the
     * queryidThresholds doesnt contain threshold, then return true and store
     * currentGain as threshold
     *
     * @param queryId
     * @param currentGain
     * @return
     */
    protected boolean adjustThreshold(String queryId, double currentGain) {
        boolean result;
        double threshold;
        if (queryidThresholds.containsKey(queryId)) {
            threshold = queryidThresholds.get(queryId);
            if (currentGain >= threshold) {
                result = true;
                threshold *= (1 + Configuration.PW_DM_THRESHOLD_ALPHA);
            } else {
                result = false;
                threshold *= (1 - Configuration.PW_DM_THRESHOLD_ALPHA);
            }
        } else {
            result = true;
            threshold = currentGain;
        }
        queryidThresholds.put(queryId, threshold);
        return result;
    }

}
