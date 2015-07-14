package de.mpii.microblogtrack.component;

import de.mpii.microblogtrack.component.core.LuceneDMConnector;
import de.mpii.microblogtrack.utility.CandidateTweet;
import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.array.TDoubleArrayList;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.log4j.Logger;
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
    protected final Map<String, LuceneDMConnector> queryTweetTrackers;

    

    public SentTweetTracker(Map<String, LuceneDMConnector> tracker) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        this.queryTweetTrackers = tracker;
    }

    /**
     * given a tweet, comparing it with all tweets being sent, if it is similar
     * with one of the sent tweet, return null
     *
     * @param tweet
     * @param qidTweetSent
     * @param queuelenlimit
     * @return
     */
    protected double[] distFilter(QueryTweetPair tweet, Map<String, PriorityQueue<CandidateTweet>> qidTweetSent, int queuelenlimit) {
        TDoubleList distances = new TDoubleArrayList();
        String queryId = tweet.queryid;
        List<CandidateTweet> tweets;
        double relativeDist;
        Vector sentVector;
        try {
            if (qidTweetSent.containsKey(queryId)) {
                tweets = new ArrayList(qidTweetSent.get(queryId));
                Vector features = tweet.vectorizeMahout();
                // the average distance among centroids as the metrics for the relative distance between tweets
                double avgCentroidDistance = 1;
                for (CandidateTweet ct : tweets) {
                    sentVector = ct.getFeature();
                    relativeDist = distanceMeasure.distance(sentVector, features) / avgCentroidDistance;
                    if (relativeDist < Configuration.DM_DIST_FILTER) {
                        ct.duplicateCount++;
                        return null;
                    }
                    distances.add(relativeDist);
                }
            }
        } catch (Exception ex) {
            logger.error("", ex);
        }
        return distances.toArray();
    }

    protected void updateSentTracker(CandidateTweet resultTweet, Map<String, PriorityQueue<CandidateTweet>> qidTweetSent, int queuelenlimit) {
        if (resultTweet.rank > 0) {
            String queryId = resultTweet.queryId;
            if (!qidTweetSent.containsKey(queryId)) {
                qidTweetSent.put(queryId, new PriorityQueue<>(queuelenlimit, new SentTweetComparator()));
            }
            // record the time that sent tweets are being added
            qidTweetSent.get(queryId).add(new CandidateTweet(resultTweet, System.currentTimeMillis()));
            // only retain the top sent tweets in the queue
            while (qidTweetSent.get(queryId).size() > queuelenlimit) {
                qidTweetSent.get(queryId).poll();
            }
        }
    }

    protected void printoutReceivedNum(String task, double count) {
        logger.info((int) count + " tweets on average [" + task + "] since start.");
    }

    protected class SentTweetComparator implements Comparator<CandidateTweet> {

        @Override
        public int compare(CandidateTweet t1, CandidateTweet t2) {
            // for some tweets that are similar to a lot of tweets, making it important to be 
            // comopared with new-coming tweets
            if (t1.duplicateCount > t2.duplicateCount) {
                return 1;
            } else if (t1.duplicateCount < t2.duplicateCount) {
                return -1;
                // prefer newer tweets
            } else if (t1.sentTimeStamp > t2.sentTimeStamp) {
                return 1;
            } else {
                return -1;
            }
        }

    }

}
