package de.mpii.microblogtrack.component;

import de.mpii.microblogtrack.utility.CandidateTweet;
import de.mpii.microblogtrack.utility.MYConstants;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import de.mpii.microblogtrack.utility.ResultTweetsTracker;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

/**
 * periodically run to pick up tweet for notification
 *
 * @author khui
 */
public class PointwiseDecisionMaker implements Runnable {

    static Logger logger = Logger.getLogger(PointwiseDecisionMaker.class);

    private final Map<String, ResultTweetsTracker> queryResultTrackers;

    private final TObjectDoubleMap<String> queryidThresholds = new TObjectDoubleHashMap<>(250);
    // dynamic threshold for the initial tweet, avoiding the most relevant tweets never come
    private final TObjectDoubleMap<String> queryidInitThresholds = new TObjectDoubleHashMap<>(250);

    private final BlockingQueue<QueryTweetPair> tweetqueue;

    private final DistanceMeasure distanceMeasure;

    private final TObjectIntMap<String> queryNumberCount = new TObjectIntHashMap<>(250);

    private final int centroidnum = 10;

    private final Set<String> finishedQueryId = new HashSet<>(250);

    // track the tweets being sent in the full duration
    private final static Map<String, List<CandidateTweet>> qidTweetSent = new HashMap<>();

    public PointwiseDecisionMaker(Map<String, ResultTweetsTracker> tracker, BlockingQueue<QueryTweetPair> tweetqueue) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        this.distanceMeasure = (DistanceMeasure) Class.forName(MYConstants.DISTANT_MEASURE_CLUSTER).newInstance();
        this.queryResultTrackers = tracker;
        this.tweetqueue = tweetqueue;
        for (String qid : tracker.keySet()) {
            queryNumberCount.put(qid, 1);
        }
    }

    @Override
    public void run() {
        for (String qid : queryResultTrackers.keySet()) {
            queryResultTrackers.get(qid).informStart2Record();
        }
        QueryTweetPair tweet = null;
        String queryid;
        while (!Thread.interrupted()) {
            try {
                tweet = tweetqueue.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ex) {
                logger.error(ex.getMessage());
            }
            if (tweet == null) {
                // logger.error("we get no tweet with past time window");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                    logger.error("", ex);
                }
            } else if (centroidnum > queryNumberCount.get(tweet.queryid)) {
                queryid = tweet.queryid;
                if (scoreFilter(tweet)) {
                    double[] distances = distFilter(tweet);
                    if (distances != null) {
                        CandidateTweet resultTweet = decisionMake(tweet, distances);
                        if (resultTweet.isSelected) {
                            queryNumberCount.adjustOrPutValue(queryid, 1, 1);
                            // write down the tweets that are notified
                            logger.info(queryNumberCount.get(queryid) + " " + resultTweet.toString() + " " + tweet.getStatus().getText() + " " + tweetqueue.size());
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
                    break;
                }
            }
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
        if (relativeScore > MYConstants.DECISION_MAKER_SCORE_FILTER) {
            isRetain = true;
        }
        return isRetain;
    }

    private double[] distFilter(QueryTweetPair tweet) {
        TDoubleList distances = new TDoubleArrayList();
        String queryId = tweet.queryid;
        List<CandidateTweet> tweets;
        double relativeDist;
        Vector sentVector;
        if (qidTweetSent.containsKey(queryId)) {
            tweets = qidTweetSent.get(queryId);
            Vector features = tweet.vectorize();
            // the average distance among centroids as the metrics for the relative distance between tweets
            double avgCentroidDistance = queryResultTrackers.get(queryId).avgDistCentroids();
            for (CandidateTweet ct : tweets) {
                sentVector = ct.getFeature();
                relativeDist = distanceMeasure.distance(sentVector, features) / avgCentroidDistance;
                if (relativeDist < MYConstants.DECISION_MAKER_DIST_FILTER) {
                    return null;
                }
                distances.add(relativeDist);
            }
        }
        return distances.toArray();
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
    public CandidateTweet decisionMake(QueryTweetPair tweet, double[] relativeDist) {
        double absoluteScore = tweet.getAbsScore();
        double relativeScore = tweet.getRelScore();
        String queryId = tweet.queryid;

        double avggain = 0;
        CandidateTweet resultTweet = new CandidateTweet(tweet.tweetid, absoluteScore, relativeScore, queryId, tweet.vectorize());
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
                resultTweet.isSelected = true;
            }
        } else {
            // if this is the first tweet to pop-up, we should make sure this is the 
            // tweet that have nearly highest relevance score, meanwhile we store this
            // relevance as threshold
            if (!queryidInitThresholds.containsKey(queryId)) {
                queryidInitThresholds.put(queryId, MYConstants.DECISION_MAKER_FIRSTPOPUP_SCORETHRESD);
            }
            double currentThread = queryidInitThresholds.get(queryId);
            if (relativeScore > currentThread) {
                avggain = absoluteScore;
                resultTweet.isSelected = true;
                adjustThreshold(queryId, avggain);
            } else {
                currentThread *= (1 - MYConstants.DECISION_MAKER_THRESHOLD_ALPHA);
                queryidInitThresholds.put(queryId, currentThread);
            }
        }
        // keep tracking all tweets being pop-up in the qidTweetSent
        if (resultTweet.isSelected) {
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
                threshold *= (1 + MYConstants.DECISION_MAKER_THRESHOLD_ALPHA);
            } else {
                result = false;
                threshold *= (1 - MYConstants.DECISION_MAKER_THRESHOLD_ALPHA);
            }
        } else {
            result = true;
            threshold = currentGain;
        }
        queryidThresholds.put(queryId, threshold);
        return result;
    }

}
