package de.mpii.microblogtrack.component;

import de.mpii.microblogtrack.utility.CandidateTweet;
import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.array.TDoubleArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

/**
 * keep tracking all tweets being sent, avoiding to send duplicate/similar
 * tweets
 *
 * @author khui
 */
public class SentTweetTracker {

    // track the tweets being sent in the full duration
    protected final static Map<String, List<CandidateTweet>> qidTweetSent = new HashMap<>();

    protected final Map<String, ResultTweetsTracker> queryResultTrackers;

    private final DistanceMeasure distanceMeasure;

    public SentTweetTracker(Map<String, ResultTweetsTracker> tracker) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        this.queryResultTrackers = tracker;
        this.distanceMeasure = (DistanceMeasure) Class.forName(Configuration.TRACKER_DISTANT_MEASURE).newInstance();
    }

    /**
     * given a tweet, comparing it with all tweets being sent, if it is similar
     * with one of the sent tweet, return null
     *
     * @param tweet
     * @return
     */
    protected double[] distFilter(QueryTweetPair tweet) {
        TDoubleList distances = new TDoubleArrayList();
        String queryId = tweet.queryid;
        List<CandidateTweet> tweets;
        double relativeDist;
        Vector sentVector;
        if (qidTweetSent.containsKey(queryId)) {
            tweets = qidTweetSent.get(queryId);
            Vector features = tweet.vectorizeMahout();
            // the average distance among centroids as the metrics for the relative distance between tweets
            double avgCentroidDistance = queryResultTrackers.get(queryId).avgDistCentroids();
            for (CandidateTweet ct : tweets) {
                sentVector = ct.getFeature();
                relativeDist = distanceMeasure.distance(sentVector, features) / avgCentroidDistance;
                if (relativeDist < Configuration.DM_DIST_FILTER) {
                    return null;
                }
                distances.add(relativeDist);
            }
        }
        return distances.toArray();
    }

}
