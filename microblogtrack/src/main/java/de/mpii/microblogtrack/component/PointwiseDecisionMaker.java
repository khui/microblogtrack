package de.mpii.microblogtrack.component;

import de.mpii.microblogtrack.utility.CandidateTweet;
import de.mpii.microblogtrack.utility.MYConstants;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import de.mpii.microblogtrack.utility.ResultTweetsTracker;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

/**
 * periodically run to pick up tweet for notification
 *
 * @author khui
 */
public class PointwiseDecisionMaker implements Callable<Void> {

    static Logger logger = Logger.getLogger(PointwiseDecisionMaker.class);

    private final Map<String, ResultTweetsTracker> queryResultTrackers;

    private final TObjectDoubleMap<String> queryidThresholds = new TObjectDoubleHashMap<>();

    private final BlockingQueue<QueryTweetPair> tweetqueue;

    private final DistanceMeasure distanceMeasure;

    private final int centroidnum = 10;
    // the number of closest tweets w.r.t. each centroid being selected
    private final int topk = 3;

    private double avgCentroidDistance = 0;
    // keep tracking the latest numberOfTweetsToKeep tweets
    //tidFeatures.put(tweetcount % numberOfTweetsToKeep, new CandidateTweet(tweetid, score, prob, this.queryid, v));
    // contain feature vector for latest RECORD_MINIUTES minutes tweets
    //private final TIntObjectMap<CandidateTweet> tidFeatures;
    //private final TIntObjectMap<double[]> tidPointwiseScores = new TIntObjectHashMap<>();
    // track the tweets being sent
    private final Map<String, List<CandidateTweet>> qidTweetSent = new HashMap<>();

    public PointwiseDecisionMaker(Map<String, ResultTweetsTracker> tracker, BlockingQueue<QueryTweetPair> tweetqueue) throws ClassNotFoundException {
        this.distanceMeasure = (DistanceMeasure) Class.forName(MYConstants.DISTANT_MEASURE_CLUSTER).newInstance();
        this.queryResultTrackers = tracker;
        this.tweetqueue = tweetqueue;
    }

    @Override
    public Void call() throws Exception {
        QueryTweetPair tweet;
        while (!Thread.interrupted()) {
            tweet = tweetqueue.poll(100, TimeUnit.MILLISECONDS);
            if (tweet != null) {
                if (scoreFilter(tweet)) {
                    double[] distances = distFilter(tweet);
                    if (distances != null) {

                    }
                }
            } else {
                logger.error("we get no tweet with past time window");
            }
        }

        for (String queryid : queryResultTrackers.keySet()) {

        }
        return null;
    }

    private boolean scoreFilter(QueryTweetPair tweet) {
        boolean isRetain = false;
        double relativeScore = tweet.getRelScore();
        if (relativeScore > 0.95) {
            isRetain = true;
        }
        return isRetain;
    }

    private double[] distFilter(QueryTweetPair tweet) {
        TDoubleList distances = new TDoubleArrayList();
        List<CandidateTweet> tweets;
        double relativeDist;
        Vector sentVector;
        if (qidTweetSent.containsKey(tweet.queryid)) {
            tweets = qidTweetSent.get(tweet.queryid);
            Vector features = tweet.vectorize();
            for (CandidateTweet ct : tweets) {
                sentVector = ct.getFeature();
                relativeDist = distanceMeasure.distance(sentVector, features) / avgCentroidDistance;
                if (relativeDist < 0.2) {
                    return null;
                }
                distances.add(relativeDist);
            }
        }
        return distances.toArray();
    }

    /**
     * "send the tweet" and keep tracking in qidTweetSent
     *
     * @param tweet
     * @param relativeDist
     * @return
     */
    public CandidateTweet decisionMake(QueryTweetPair tweet, double[] relativeDist) {
        double absoluteScore = tweet.getAbsScore();
        double avggain = 0;
        CandidateTweet resultTweet;
        if (relativeDist.length > 0) {
            for (double dist : relativeDist) {
                avggain += dist * absoluteScore;
            }
            avggain /= (double) relativeDist.length;
            if (avggain >= queryidThresholds.get(tweet.queryid)) {

            }
        } else {
            double relativeScore = tweet.getRelScore();
            if (relativeScore > 0.995) {
                avggain = absoluteScore;
                qidTweetSent.put(tweet.queryid, new ArrayList<>());
                resultTweet = new CandidateTweet(tweet.tweetid, tweet.getAbsScore(), relativeScore, true, tweet.queryid, tweet.vectorize());
                qidTweetSent.get(tweet.queryid).add(new CandidateTweet(resultTweet));
                queryidThresholds.put(tweet.queryid, avggain);
            }
        }
        return null;
    }

}
