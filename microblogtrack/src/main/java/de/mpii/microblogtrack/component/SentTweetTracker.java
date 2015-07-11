package de.mpii.microblogtrack.component;

import de.mpii.microblogtrack.component.core.ResultTweetsTracker;
import de.mpii.microblogtrack.utility.CandidateTweet;
import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.array.TDoubleArrayList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

/**
 * keep tracking all tweets being sent, avoiding to send duplicate/similar
 * tweets
 *
 * @author khui
 */
public class SentTweetTracker {

    static Logger logger = Logger.getLogger(SentTweetTracker.class.getName());

    // track the tweets being sent in the full duration
    //protected final static Map<String, List<CandidateTweet>> qidTweetSent = Collections.synchronizedMap(new HashMap<>());
    protected final Map<String, ResultTweetsTracker> queryTweetTrackers;

    private final DistanceMeasure distanceMeasure;

    public SentTweetTracker(Map<String, ResultTweetsTracker> tracker) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        this.queryTweetTrackers = tracker;
        this.distanceMeasure = (DistanceMeasure) Class.forName(Configuration.TRACKER_DISTANT_MEASURE).newInstance();
    }

    /**
     * given a tweet, comparing it with all tweets being sent, if it is similar
     * with one of the sent tweet, return null
     *
     * @param tweet
     * @param qidTweetSent
     * @return
     */
    protected double[] distFilter(QueryTweetPair tweet, Map<String, List<CandidateTweet>> qidTweetSent) {
        TDoubleList distances = new TDoubleArrayList();
        String queryId = tweet.queryid;
        List<CandidateTweet> tweets;
        double relativeDist;
        Vector sentVector;

        if (qidTweetSent.containsKey(queryId)) {
            tweets = qidTweetSent.get(queryId);
            Vector features = tweet.vectorizeMahout();
            // the average distance among centroids as the metrics for the relative distance between tweets
            double avgCentroidDistance = queryTweetTrackers.get(queryId).avgDistCentroids();
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

    protected void updateSentTracker(CandidateTweet resultTweet, Map<String, List<CandidateTweet>> qidTweetSent) {
        if (resultTweet.rank > 0) {
            String queryId = resultTweet.queryId;
            if (!qidTweetSent.containsKey(queryId)) {
                qidTweetSent.put(queryId, new ArrayList<>());
            }
            qidTweetSent.get(queryId).add(new CandidateTweet(resultTweet));
        }
    }

    protected void printoutReceivedNum(String task, double count) {
        logger.info((int) count + " tweets on average [" + task + "] since start.");
    }

}
