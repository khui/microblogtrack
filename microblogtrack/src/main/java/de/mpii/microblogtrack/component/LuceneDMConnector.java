package de.mpii.microblogtrack.component;

import de.mpii.microblogtrack.component.core.*;
import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import gnu.trove.TCollections;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.TDoubleIntMap;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TDoubleIntHashMap;
import java.util.Collection;
import org.apache.log4j.Logger;

/**
 * main facility for ranking and pointwise prediction. for each query, we
 * construct one ResultTweetsTracker instance, keeping track of the relevant
 * tweets for this query, meanwhile keeping track of the centroid by streaming
 * k-means..
 *
 * @author khui
 */
public class LuceneDMConnector implements ScoreTracker {

    static Logger logger = Logger.getLogger(ResultTweetsTracker.class);

    public final String queryid;

    // record the occrrence for each predict score, generating the approximating cumulative distribution
    private final TDoubleIntMap predictScoreTracker = TCollections.synchronizedMap(new TDoubleIntHashMap());

    // identification for each tweet
    private volatile int tweetcount = 0;

    // pointwise decision maker always need to wait amont of time
    // before make decision
    private volatile boolean pwQueue2receiveTweets = false;
    // listwise decision keep track of all tweets within one day
    // after start (set true), this flag will not change back to false
    private volatile boolean lwQueue2receiveTweets = false;

    public LuceneDMConnector(String queryid) {
        this.queryid = queryid;
    }

    @Override
    public void addTweets(Collection<QueryTweetPair> qtps) {
        double absoluteScore, relativeScore;
        for (QueryTweetPair qtp : qtps) {
            tweetcount++;
            // the unique predicting score for one tweet, and the corresponding cumulative prob is used as vector weight
            absoluteScore = trackPredictScore(qtp.getPredictRes());
            // add the tweet to the clustering, using the tweet count as the centroid key
            relativeScore = getCumulativeProb(absoluteScore);
            qtp.setPredictScore(Configuration.PRED_RELATIVESCORE, relativeScore);
        }
    }

    @Override
    public double relativeScore(double absoluteScore) {
        return getCumulativeProb(absoluteScore);
    }

    public synchronized boolean whetherOffer2PWQueue() {
        return pwQueue2receiveTweets;
    }

    public synchronized boolean whetherOffer2LWQueue() {
        return lwQueue2receiveTweets;
    }

    public synchronized void offer2PWQueue() {
        pwQueue2receiveTweets = true;
    }

    public synchronized void ceasePWQueue() {
        pwQueue2receiveTweets = false;
    }

    public synchronized void offer2LWQueue() {
        lwQueue2receiveTweets = true;
    }

    /**
     * return the cumulative probability for the top
     * Configuration.TRACKER_CUMULATIVE_TOPPERC tweets
     *
     * @param score
     * @return
     */
    private double getCumulativeProb(double score) {
        // default value
        double prob = 1 - Configuration.TRACKER_CUMULATIVE_TOPPERC;
        int cumulativeCount = 0;

        int currentTweetCount = this.tweetcount;
        // when we dont have enough tweets, we return the default directly as relative score
        if (currentTweetCount < Configuration.TRACKER_CUMULATIVE_GRANULARITY) {
            return prob;
        }
        // we only compute the top Configuration.TRACKER_CUMULATIVE_TOPPERC percent for efficiency reason 
        double topNumber = currentTweetCount * Configuration.TRACKER_CUMULATIVE_TOPPERC;
        TDoubleIntMap copyOfScoreTracker;
        synchronized (predictScoreTracker) {
            copyOfScoreTracker = new TDoubleIntHashMap(predictScoreTracker);
        }
        TDoubleList scores = new TDoubleArrayList(copyOfScoreTracker.keys());
        while (scores.size() > 0) {
            double minV = scores.min();
            if (score <= minV) {
                break;
            }
            double maxV = scores.max();
            cumulativeCount += copyOfScoreTracker.get(maxV);
            scores.remove(maxV);
            if (cumulativeCount >= topNumber || score >= maxV) {
                // for absolute score little than maxV, use the default value
                if (score >= maxV) {
                    prob = 1 - (double) cumulativeCount / currentTweetCount;
                }
                break;
            }
        }
        return prob;
    }

    private double trackPredictScore(TObjectDoubleMap<String> predictScores) {
        String[] scorenames = new String[]{Configuration.PRED_ABSOLUTESCORE};
        double[] scores = new double[scorenames.length];
        double absoluteScore = 0;
        for (int i = 0; i < scorenames.length; i++) {
            if (predictScores.containsKey(scorenames[i])) {
                scores[i] = predictScores.get(scorenames[i]);
                if (scorenames[i].equals(Configuration.PRED_ABSOLUTESCORE)) {
                    absoluteScore = (double) Math.round(scores[i] * Configuration.TRACKER_CUMULATIVE_GRANULARITY) / Configuration.TRACKER_CUMULATIVE_GRANULARITY;
                    synchronized (predictScoreTracker) {
                        predictScoreTracker.adjustOrPutValue(absoluteScore, 1, 1);
                    }
                }
            }
        }
        return absoluteScore;
    }

}
