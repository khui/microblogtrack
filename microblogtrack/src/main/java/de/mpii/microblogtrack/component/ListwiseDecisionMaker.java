package de.mpii.microblogtrack.component;

import de.mpii.maxrep.MaxRep;
import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.logging.Level;
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
public class ListwiseDecisionMaker implements Runnable {

    static Logger logger = Logger.getLogger(ListwiseDecisionMaker.class.getName());

    private final PriorityBlockingQueue<QueryTweetPair> tweetqueue;

    public ListwiseDecisionMaker(PriorityBlockingQueue<QueryTweetPair> tweetqueue) {
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
                    printListWise(decisionMakeMaxRep());
                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
                    logger.error("", ex);
                }
                break;
            }
            // we simply collect top-LW_DM_QUEUE_LEN tweets with highest prediction score 
            if (tweetqueue.size() > Configuration.LW_DM_QUEUE_LEN) {
                tweetqueue.poll();
            }
        }
    }

    private List<QueryTweetPair> decisionMakeMaxRep() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        List<QueryTweetPair> candidateTweets = new ArrayList<>();
        int tweetnum = tweetqueue.drainTo(candidateTweets);
        Centroid[] points2select = new Centroid[tweetnum];
        logger.info("ListwiseDecisionMaker is interrupted, " + tweetnum + " tweets have drained to list");
        int i = 0;
        for (QueryTweetPair qtp : candidateTweets) {
            points2select[i++] = new Centroid(i, qtp.vectorizeMahout(), qtp.getAbsScore());
        }
        MaxRep selector = new MaxRep(points2select);
        int[] selectedIndex = selector.selectMaxRep(Configuration.LW_DM_SELECTNUM);
        List<QueryTweetPair> selectedQTPs = new ArrayList<>();
        for (int index : selectedIndex) {
            selectedQTPs.add(candidateTweets.get(index));
        }
        return selectedQTPs;
    }

    private void printListWise(List<QueryTweetPair> qtps) {

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
