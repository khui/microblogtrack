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
import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

    private final static Map<String, List<CandidateTweet>> qidTweetSent = Collections.synchronizedMap(new HashMap<>());

    private final BlockingQueue<QueryTweetPair> tweetqueue;

    private final TObjectDoubleMap<String> queryidThresholds = new TObjectDoubleHashMap<>(250);
    // dynamic threshold for the initial tweet, avoiding the most relevant tweets never come
    private final TObjectDoubleMap<String> queryidInitThresholds = new TObjectDoubleHashMap<>(250);

    private final int centroidnum = 10;

    private final ResultPrinter resultprinter;

    public PointwiseDecisionMaker(Map<String, ResultTweetsTracker> tracker, BlockingQueue<QueryTweetPair> tweetqueue, ResultPrinter resultprinter) throws ClassNotFoundException, InstantiationException, IllegalAccessException, FileNotFoundException {
        super(tracker);
        this.tweetqueue = tweetqueue;
        this.resultprinter = resultprinter;
    }

    @Override
    public void run() {
        // only after receiving enough tweets, the tweets will be considered to 
        // be reported
        int num_received_since_start = 0;
        int num_filtered_distance = 0;

        for (String qid : queryTweetTrackers.keySet()) {
            queryTweetTrackers.get(qid).offer2PWQueue();
        }
        logger.info("PW-DM started");
        TObjectIntMap<String> queryNumberCount = new TObjectIntHashMap<>(250);
        Set<String> finishedQueryId = new HashSet<>(250);
        QueryTweetPair tweet;
        String queryid;
        /**
         * check the interrupt flag in each loop, since this is how we terminate
         * the decision maker from the timer if after one day, there still exist
         * query has less than 10 results.
         */
        while (true) {
            if (Thread.interrupted()) {
                printoutReceivedNum("received in PW-DM", num_received_since_start);
                printoutReceivedNum("filtered by distance in PW-DM", num_filtered_distance);
                clear();
                return;
            }
            tweet = tweetqueue.poll();
            if (tweet == null) {
                continue;
            }

            num_received_since_start++;

            // make decision untill we have receive enough tweets
            if (num_received_since_start < Configuration.PW_DW_CUMULATECOUNT_DELAY) {
                continue;
            }
            if (centroidnum > queryNumberCount.get(tweet.queryid)) {
                queryid = tweet.queryid;
                if (scoreFilter(tweet)) {
                    double[] distances;
                    synchronized (qidTweetSent) {
                        distances = distFilter(tweet, qidTweetSent);
                    }
                    if (distances != null) {
                        CandidateTweet resultTweet = decisionMake(tweet, distances, queryNumberCount);
                        if (resultTweet.rank > 0) {
                            try {
                                // write down the tweets that are notified
                                resultprinter.println(queryid, resultTweet.forDebugToString(""));
                                resultprinter.printlog(queryid, tweet.getStatus().getText(), resultTweet.absoluteScore, resultTweet.relativeScore);
                            } catch (FileNotFoundException | ParseException ex) {
                                logger.error("", ex);
                            }
                            queryNumberCount.adjustOrPutValue(queryid, 1, 1);
                            //logger.info(queryNumberCount.get(queryid) + " " + resultTweet.toString() + " " + tweet.getStatus().getText() + " " + tweetqueue.size());
                        } else {
                            //logger.info("tweet has not been selected: " + tweet.getRelScore() + "  " + tweet.getAbsScore() + " " + queryidThresholds.get(tweet.queryid));
                        }
                    } else {
                        num_filtered_distance++;
                        //logger.info("filter out the tweet by distance: " + qidTweetSent.get(tweet.queryid).size());
                    }
                } else {
                    //logger.info("filter out the tweet by score: " + tweet.getRelScore());
                }
            } else {
                //logger.info("Finished: " + tweet.queryid + " " + queryNumberCount.get(tweet.queryid));
                finishedQueryId.add(tweet.queryid);
                if (finishedQueryId.size() >= queryNumberCount.size()) {
                    logger.info("Finished all in PW-DM in this round: " + finishedQueryId.size());
                    clear();
                    break;
                }
            }
        }

    }

    private void clear() {
        resultprinter.flush();
        for (String qid : queryTweetTrackers.keySet()) {
            queryTweetTrackers.get(qid).ceasePWQueue();
        }
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
     * @param queryNumberCount
     * @return
     */
    protected CandidateTweet decisionMake(QueryTweetPair tweet, double[] relativeDist, TObjectIntMap<String> queryNumberCount) {
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
                resultTweet.rank = queryNumberCount.get(queryId) + 1;
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
                resultTweet.rank = queryNumberCount.get(queryId) + 1;
                adjustThreshold(queryId, avggain);
            } else {
                currentThread *= (1 - Configuration.PW_DM_THRESHOLD_ALPHA);
                queryidInitThresholds.put(queryId, currentThread);
            }
        }
        // keep tracking all tweets being pop-up in the qidTweetSent
        synchronized (qidTweetSent) {
            updateSentTracker(resultTweet, qidTweetSent);
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
        //logger.info("threahold " + queryidThresholds.get(queryId) + " " + queryidInitThresholds.get(queryId));
        queryidThresholds.put(queryId, threshold);
        return result;
    }

}
