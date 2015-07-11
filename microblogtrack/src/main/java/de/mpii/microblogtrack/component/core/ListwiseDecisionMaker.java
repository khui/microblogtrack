package de.mpii.microblogtrack.component.core;

import de.mpii.microblogtrack.component.core.ResultTweetsTracker;
import de.mpii.lowcosteval.maxrep.MaxRep;
import de.mpii.microblogtrack.component.SentTweetTracker;
import de.mpii.microblogtrack.utility.CandidateTweet;
import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import de.mpii.microblogtrack.utility.io.printresult.ResultPrinter;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.IntStream;
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

    private final static Map<String, List<CandidateTweet>> qidTweetSent = Collections.synchronizedMap(new HashMap<>());

    private final BlockingQueue<QueryTweetPair> tweetqueue;

    private final ResultPrinter resultprinter;

    public ListwiseDecisionMaker(Map<String, ResultTweetsTracker> tracker, BlockingQueue<QueryTweetPair> tweetqueue, ResultPrinter resultprinter) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        super(tracker);
        this.tweetqueue = tweetqueue;
        this.resultprinter = resultprinter;
    }

    @Override
    public void run() {
        TObjectIntMap<String> qid_tweetnum = new TObjectIntHashMap<>();
        TObjectIntMap<String> num_filtered_distance = new TObjectIntHashMap<>();
        Map<String, PriorityBlockingQueue<QueryTweetPair>> qidQueue = new HashMap<>(250);
        logger.info("LW-DM started");
        for (String qid : queryTweetTrackers.keySet()) {
            if (!queryTweetTrackers.get(qid).whetherOffer2LWQueue()) {
                queryTweetTrackers.get(qid).offer2LWQueue();
            }
        }
        /**
         * check the interrupt flag in each loop, since this is how we terminate
         * the decision maker from the timer after one day, thereafter we start
         * our decision making method to pop-up top-100 tweets for email digest.
         */
        while (true) {
            if (Thread.interrupted()) {
                printoutReceivedNum("received in LW-DM", IntStream.of(qid_tweetnum.values()).average().getAsDouble());
                printoutReceivedNum("filtered by distance in LW-DM", IntStream.of(num_filtered_distance.values()).average().getAsDouble());
                try {
                    for (String qid : qidQueue.keySet()) {
                        List<CandidateTweet> tweets = decisionMakeMaxRep(qidQueue.get(qid));
                        if (tweets == null) {
                            continue;
                        }
                        for (CandidateTweet tweet : tweets) {
                            resultprinter.println(qid, tweet.forDebugToString(""));
                            resultprinter.printlog(qid, tweet.getTweetStr(), tweet.absoluteScore, tweet.relativeScore);
                        }
                    }
                    resultprinter.flush();
                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | FileNotFoundException | ParseException ex) {
                    logger.error("", ex);
                }
                return;
            }

            try {
                // we simply collect top-LW_DM_QUEUE_LEN tweets with highest prediction score
                // for each query
                QueryTweetPair qtp = tweetqueue.poll();

                // if fetch no tweet from the queue
                if (qtp == null) {
                    continue;
                }
                qid_tweetnum.adjustOrPutValue(qtp.queryid, 1, 1);

                double relativeScore = qtp.getRelScore();
                if (relativeScore == 0) {
                    logger.error("LW-DW, relativeScore:" + relativeScore);
                }

                synchronized (qidTweetSent) {
                    // if the tweet is too similar with one of the already sent tweet
                    if (distFilter(qtp, qidTweetSent) == null) {
                        num_filtered_distance.adjustOrPutValue(qtp.queryid, 1, 1);
                        continue;
                    }
                }

                String qid = qtp.queryid;
                if (!qidQueue.containsKey(qid)) {
                    qidQueue.put(qid, getPriorityQueue());
                }

                qidQueue.get(qid).offer(new QueryTweetPair(qtp));
                if (qidQueue.get(qid).size() > Configuration.LW_DM_QUEUE_LEN) {
                    qidQueue.get(qid).poll();
                }
            } catch (Exception ex) {
                logger.error("", ex);
            }

        }
    }

    private List<CandidateTweet> decisionMakeMaxRep(PriorityBlockingQueue<QueryTweetPair> queue) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        List<QueryTweetPair> candidateTweets = new ArrayList<>();
        int tweetnum = queue.drainTo(candidateTweets);
        if (tweetnum <= 0) {
            logger.error("The candidate tweet list is empty");
            return null;
        }
        List<Centroid> points2select = new ArrayList<>(tweetnum);
        // pick up the maximum and minimum absolute score
        double maxAbsoluteScore = Double.MIN_VALUE;
        double minAbsoluteScore = Double.MAX_VALUE;
        for (QueryTweetPair candidateTweet : candidateTweets) {
            double absolutescore = candidateTweet.getAbsScore();
            if (absolutescore > maxAbsoluteScore) {
                maxAbsoluteScore = absolutescore;
            }
            if (absolutescore < minAbsoluteScore) {
                minAbsoluteScore = absolutescore;
            }
        }
        double difference = maxAbsoluteScore - minAbsoluteScore;
        double weight;
        for (int i = 0; i < candidateTweets.size(); i++) {
            if (difference > 0) {
                weight = Configuration.LW_DM_WEIGHT_MINW + (1 - Configuration.LW_DM_WEIGHT_MINW)
                        * (candidateTweets.get(i).getAbsScore() - minAbsoluteScore) / difference;
            } else {
                weight = 1;
            }
            points2select.add(new Centroid(i, candidateTweets.get(i).vectorizeMahout(), weight));
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
            selectedQTPs.get(selectedQTPs.size() - 1).setTweetStr(candidateTweets.get(index).getStatus().getText());
        }
        for (CandidateTweet resultTweet : selectedQTPs) {
            synchronized (qidTweetSent) {
                updateSentTracker(resultTweet, qidTweetSent);
            }
        }
        return selectedQTPs;
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
