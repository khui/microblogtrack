package de.mpii.microblogtrack.component.core;

import de.mpii.microblogtrack.component.SentTweetTracker;
import de.mpii.microblogtrack.utility.CandidateTweet;
import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import de.mpii.microblogtrack.utility.io.printresult.ResultPrinter;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.IntStream;
import org.apache.log4j.Logger;

/**
 *
 * @author khui
 */
public abstract class ListwiseDecisionMaker extends SentTweetTracker implements Runnable {

    static Logger logger = Logger.getLogger(ListwiseDecisionMakerMapRep.class.getName());
    protected static final Map<String, PriorityQueue<CandidateTweet>> qidSentTweetQueues = Collections.synchronizedMap(new HashMap<>());

    protected final BlockingQueue<QueryTweetPair> qtpQueue2Process;
    protected final ResultPrinter resultprinter;

    public ListwiseDecisionMaker(Map<String, LuceneDMConnector> tracker, BlockingQueue<QueryTweetPair> tweetqueue, ResultPrinter resultprinter) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        super(tracker);
        this.qtpQueue2Process = tweetqueue;
        this.resultprinter = resultprinter;
    }

    @Override
    public void run() {
        TObjectIntMap<String> qid_tweetnum_received = new TObjectIntHashMap<>();
        TObjectIntMap<String> num_filteredby_similarity = new TObjectIntHashMap<>();
        Map<String, PriorityBlockingQueue<QueryTweetPair>> PriorityQueue4FurtherProcessing = new HashMap<>(250);
        logger.info("LW-DM started");
        for (String qid : queryRelativeScoreTracker.keySet()) {
            // to notify the lucene components to put the retrieved tweets into the queue
            if (!queryRelativeScoreTracker.get(qid).whetherOffer2LWQueue()) {
                queryRelativeScoreTracker.get(qid).offer2LWQueue();
            }
        }
        /**
         * check the interrupt flag in each loop, since this is how we terminate
         * the decision maker from the timer after one day, thereafter we start
         * our decision making method to pop-up top-100 tweets for email digest.
         */
        while (true) {
            if (Thread.interrupted()) {
                try {
                    int[] dists = qid_tweetnum_received.values();
                    if (dists.length > 0) {
                        printoutReceivedNum("received in LW-DM ", IntStream.of(dists).average().getAsDouble());
                    } else {
                        printoutReceivedNum("received in LW-DM ", 0);
                    }
                    dists = num_filteredby_similarity.values();
                    if (dists.length > 0) {
                        printoutReceivedNum("filtered by similarity in LW-DM", IntStream.of(dists).average().getAsDouble());
                    } else {
                        printoutReceivedNum("filtered by similarity in LW-DM", 0);
                    }
                } catch (Exception ex) {
                    logger.error("", ex);
                }
                try {
                    TObjectIntMap<String> qid_pasttomaxrep = new TObjectIntHashMap<>();
                    for (String qid : PriorityQueue4FurtherProcessing.keySet()) {
                        qid_pasttomaxrep.put(qid, PriorityQueue4FurtherProcessing.get(qid).size());
                        List<CandidateTweet> resultTweets = decisionMake(PriorityQueue4FurtherProcessing.get(qid), qid);
                        if (resultTweets == null) {
                            logger.error("none tweets selected in LW-DM for " + qid);
                            continue;
                        }
                        for (int rank = 1; rank <= resultTweets.size(); rank++) {
                            CandidateTweet tweet = resultTweets.get(rank - 1);
                            resultprinter.printResult(qid, tweet.digestOutput(rank));
                            resultprinter.printlog(qid, tweet.getTweetText(), tweet.getUrlTitleText(), tweet.getAbsScore(), tweet.getRelScore());
                        }
                    }
                    resultprinter.flush();
                    int[] passed_tweet_num = qid_pasttomaxrep.values();
                    if (passed_tweet_num.length > 0) {
                        printoutReceivedNum("passed to decision maker method in LW-DM", IntStream.of(passed_tweet_num).average().getAsDouble());
                    } else {
                        printoutReceivedNum("passed to decision maker method in LW-DM", 0);
                    }
                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | FileNotFoundException | ParseException | IllegalArgumentException ex) {
                    logger.error("", ex);
                } catch (Exception ex) {
                    logger.error("", ex);
                }
                logger.info("LW-DM: finished and get out");
                return;
            }
            try {
                // we simply collect top-LW_DM_QUEUE_LEN tweets with highest prediction score
                // for each query
                QueryTweetPair qtp = qtpQueue2Process.poll();
                // if fetch no tweet from the queue
                if (qtp == null) {
                    continue;
                }
                String qid = qtp.queryid;
                qid_tweetnum_received.adjustOrPutValue(qid, 1, 1);
                synchronized (qidSentTweetQueues) {
                    // if the tweet is too similar with one of the already sent tweet
                    if (compare2SentTweetFilger(qtp, qidSentTweetQueues) == null) {
                        num_filteredby_similarity.adjustOrPutValue(qid, 1, 1);
                        continue;
                    }
                }
                if (!PriorityQueue4FurtherProcessing.containsKey(qid)) {
                    PriorityQueue4FurtherProcessing.put(qid, getPriorityQueue());
                }
                PriorityQueue4FurtherProcessing.get(qid).offer(new QueryTweetPair(qtp));
                if (PriorityQueue4FurtherProcessing.get(qid).size() > Configuration.LW_DM_QUEUE2PROCESS_LEN) {
                    PriorityQueue4FurtherProcessing.get(qid).poll();
                }
            } catch (Exception ex) {
                logger.error("", ex);
            }
        }
    }

    /**
     * generate the priority queue required in the communication of the
     * lucenescorer component and this list wise decision making component
     *
     * @return
     */
    public static PriorityBlockingQueue<QueryTweetPair> getPriorityQueue() {
        int size = Configuration.LW_DM_QUEUE2PROCESS_LEN;
        PriorityBlockingQueue<QueryTweetPair> queue = new PriorityBlockingQueue<>(size, new Comparator<QueryTweetPair>() {
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

    protected abstract List<CandidateTweet> decisionMake(PriorityBlockingQueue<QueryTweetPair> queue, String qid) throws Exception;

}
