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

/**
 * keep tracking all tweets being sent, avoiding to send duplicate/similar
 * tweets
 *
 * @author khui
 */
public class SentTweetTracker {

    static Logger logger = Logger.getLogger(SentTweetTracker.class.getName());

    private final TweetSimilarity similarityMeasure;

    protected final Map<String, LuceneDMConnector> queryRelativeScoreTracker;

    public SentTweetTracker(Map<String, LuceneDMConnector> tracker) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        this.queryRelativeScoreTracker = tracker;
        this.similarityMeasure = new TweetStringSimilarity();
    }

    /**
     * given a tweet, comparing it with all tweets being sent, if it is similar
     * with one of the sent tweet, return null
     *
     * @param tweet
     * @param qidTweetSent
     * @return
     */
    protected double[] compare2SentTweetFilger(QueryTweetPair tweet, Map<String, PriorityQueue<CandidateTweet>> qidTweetSent) {
        TDoubleList similarities = new TDoubleArrayList();
        String queryId = tweet.queryid;
        List<CandidateTweet> tweets;
        double similarity;
        try {
            if (qidTweetSent.containsKey(queryId)) {
                tweets = new ArrayList(qidTweetSent.get(queryId));
                // the average distance among centroids as the metrics for the relative distance between tweets
                for (CandidateTweet sentTeet : tweets) {
                    similarity = similarityMeasure.similarity(sentTeet, tweet);
                    if (similarity >= Configuration.DM_SIMILARITY_FILTER) {
                        sentTeet.duplicateCount++;
                        return null;
                    }
                    similarities.add(similarity);
                }
            }
        } catch (Exception ex) {
            logger.error("", ex);
        }
        return similarities.toArray();
    }

    protected void updateSentTracker(CandidateTweet resultTweet, Map<String, PriorityQueue<CandidateTweet>> qidTweetSent, int queuelenlimit) {
        if (resultTweet.rank > 0) {
            resultTweet.setTimeStamp();
            String queryId = resultTweet.queryid;
            if (!qidTweetSent.containsKey(queryId)) {
                qidTweetSent.put(queryId, new PriorityQueue<>(queuelenlimit, new SentTweetComparator()));
            }
            // record the time that sent tweets are being added
            qidTweetSent.get(queryId).add(new CandidateTweet(resultTweet));
            // only retain the top sent tweets in the queue
            while (qidTweetSent.get(queryId).size() > queuelenlimit) {
                qidTweetSent.get(queryId).poll();
            }
        }
    }

    protected void printoutReceivedNum(String task, double count) {
        logger.info((int) count + " tweets on average [" + task + "]");
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
            } else if (t1.millis_send_timestamp > t2.millis_send_timestamp) {
                return 1;
            } else {
                return -1;
            }
        }

    }

}
