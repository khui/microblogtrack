package de.mpii.microblogtrack.component;

import de.mpii.maxrep.MaxRep;
import de.mpii.microblogtrack.utility.CandidateTweet;
import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.log4j.Logger;
import org.apache.mahout.math.Centroid;

/**
 * collect tweets from lucene, thereafter return top-100 list at the end of day.
 * in particular, we keep track the first k documents with highest score since
 * today, at the end of each day we select top-100 tweets to generate output for
 * e-mail digest
 *
 * @author khui
 */
public class ListwiseDecisionMaker extends SentTweetTracker implements Runnable {

    static Logger logger = Logger.getLogger(ListwiseDecisionMaker.class.getName());

    private final BlockingQueue<QueryTweetPair> tweetqueue;

    private final Map<String, PriorityBlockingQueue<QueryTweetPair>> qidQueue = new HashMap<>(250);

    public ListwiseDecisionMaker(Map<String, ResultTweetsTracker> tracker, BlockingQueue<QueryTweetPair> tweetqueue) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        super(tracker);
        this.tweetqueue = tweetqueue;
    }

    @Override
    public void run() {
        /**
         * check the interrupt flag in each loop, since this is how we terminate
         * the decision maker from the timer after one day, thereafter we start
         * our decision making method to pop-up top-100 tweets for email digest.
         */
        while (true) {
            if (Thread.interrupted()) {
                try {
                    for (String qid : qidQueue.keySet()) {
                        decisionMakeMaxRep(qidQueue.get(qid), qid);
                    }
                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
                    logger.error("", ex);
                }
                break;
            }
            // we simply collect top-LW_DM_QUEUE_LEN tweets with highest prediction score
            // for each query
            QueryTweetPair qtp = tweetqueue.poll();
            // if fetch no tweet from the queue
            if (qtp == null) {
                continue;
            }
            // if the fetched tweets have two low relative score, indicating
            // it is barely relevant
            if (qtp.getRelScore() < Configuration.LW_DM_SCORE_FILTER) {
                continue;
            }
            // if the tweet is too similar with one of the already sent tweet
            if (distFilter(qtp) == null) {
                continue;
            }
            String qid = qtp.queryid;
            if (!qidQueue.containsKey(qid)) {
                qidQueue.put(qid, getPriorityQueue());
            }

            qidQueue.get(qid).offer(qtp);
            if (qidQueue.get(qid).size() > Configuration.LW_DM_QUEUE_LEN) {
                qidQueue.get(qid).poll();
            }
        }
    }

    private List<CandidateTweet> decisionMakeMaxRep(PriorityBlockingQueue<QueryTweetPair> queue, String qid) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        List<QueryTweetPair> candidateTweets = new ArrayList<>();
        int tweetnum = queue.drainTo(candidateTweets);
        Centroid[] points2select = new Centroid[tweetnum];
        logger.info("ListwiseDecisionMaker is interrupted, " + tweetnum + " for " + qid + " tweets have drained to list");
        for (int i = 0; i < candidateTweets.size(); i++) {
            points2select[i++] = new Centroid(i, candidateTweets.get(i).vectorizeMahout(), candidateTweets.get(i).getAbsScore());
        }
        MaxRep selector = new MaxRep(points2select);
        int[] selectedIndex = selector.selectMaxRep(Configuration.LW_DM_SELECTNUM);
        List<CandidateTweet> selectedQTPs = new ArrayList<>();
        int rank = 1;
        for (int index : selectedIndex) {
            selectedQTPs.add(new CandidateTweet(candidateTweets.get(index).tweetid,
                    candidateTweets.get(index).getAbsScore(),
                    candidateTweets.get(index).getRelScore(),
                    rank++,
                    candidateTweets.get(index).queryid,
                    candidateTweets.get(index).vectorizeMahout()));
        }
        return selectedQTPs;
    }

    private void printListWise(List<QueryTweetPair> qtps, String qid) {

    }

    /**
     * generate the priority queue required in the communication of the
     * lucenescorer component and this list wise decision making component
     *
     * @return
     */
    public static PriorityBlockingQueue<QueryTweetPair> getPriorityQueue() {
        int size = Configuration.LW_DM_QUEUE_LEN * 2;
        PriorityBlockingQueue<QueryTweetPair> queue
                = new PriorityBlockingQueue<>(size, new Comparator<QueryTweetPair>() {
                    /**
                     * compare according to the absolute prediction score
                     *
                     * @param qtp1
                     * @param qtp2
                     * @return
                     */
                    @Override
                    public int compare(QueryTweetPair qtp1, QueryTweetPair qtp2) {
                        if (qtp1.getAbsScore() > qtp2.getAbsScore()) {
                            return 1;
                        } else if (qtp1.getAbsScore() < qtp2.getAbsScore()) {
                            return -1;
                        } else if (qtp1.tweetid > qtp2.tweetid) {
                            // use the tweetid to break ties
                            return 1;
                        } else {
                            return -1;
                        }
                    }
                });

        return queue;
    }

}
