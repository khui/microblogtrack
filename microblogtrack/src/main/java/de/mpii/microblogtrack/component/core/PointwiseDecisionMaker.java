package de.mpii.microblogtrack.component.core;

import de.mpii.microblogtrack.component.SentTweetTracker;
import de.mpii.microblogtrack.utility.CandidateTweet;
import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import de.mpii.microblogtrack.utility.io.printresult.ResultPrinter;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.stream.IntStream;
import org.apache.log4j.Logger;

/**
 * periodically run to pick up qtp for notification
 *
 * @author khui
 */
public class PointwiseDecisionMaker extends SentTweetTracker implements Runnable {

    static Logger logger = Logger.getLogger(PointwiseDecisionMaker.class);

    private final static Map<String, PriorityQueue<CandidateTweet>> qidSentTweetQueues = Collections.synchronizedMap(new HashMap<>());

    private final BlockingQueue<QueryTweetPair> qtpQueue2Process;

    private final TObjectDoubleMap<String> qidAbsThreshold2SentTweet = new TObjectDoubleHashMap<>(250);
    // dynamic threshold for the initial qtp, avoiding the most relevant tweets never come
    private final TObjectDoubleMap<String> qidInitRelativeThread2SentFirstTweet = new TObjectDoubleHashMap<>(250);

    private final ResultPrinter resultprinter;

    public PointwiseDecisionMaker(Map<String, LuceneDMConnector> tracker, BlockingQueue<QueryTweetPair> incomingQtpQueue, ResultPrinter resultprinter) throws ClassNotFoundException, InstantiationException, IllegalAccessException, FileNotFoundException {
        super(tracker);
        this.qtpQueue2Process = incomingQtpQueue;
        this.resultprinter = resultprinter;
    }

    @Override
    public void run() {
        TObjectIntMap<String> qidNotificationNumTracker = new TObjectIntHashMap<>(250);
        Set<String> finishNotificationQidTracker = new HashSet<>(250);
        QueryTweetPair qtp;
        String queryid;
        // only after receiving enough tweets, the tweets will be considered to 
        // be reported
        TObjectIntMap<String> qid_tweetnum_received = new TObjectIntHashMap<>();
        TObjectIntMap<String> num_filteredby_similarity = new TObjectIntHashMap<>();
        // to notify the lucene components to put the retrieved tweets into the queue
        for (String qid : queryRelativeScoreTracker.keySet()) {
            if (!queryRelativeScoreTracker.get(qid).whetherOffer2PWQueue()) {
                queryRelativeScoreTracker.get(qid).offer2PWQueue();
            }
        }
        logger.info("PW-DM started");

        /**
         * check the interrupt flag in each loop, since this is how we terminate
         * the decision maker from the timer if after one day, there still exist
         * query has less than 10 results.
         */
        while (true) {
            if (Thread.interrupted()) {
                try {
                    clear();
                    int[] dists = qid_tweetnum_received.values();
                    if (dists.length > 0) {
                        printoutReceivedNum("received in PW-DM", IntStream.of(dists).average().getAsDouble());
                    } else {
                        printoutReceivedNum("received in PW-DM", 0);
                    }
                    dists = num_filteredby_similarity.values();
                    if (dists.length > 0) {
                        printoutReceivedNum("filtered by similarity in PW-DM", IntStream.of(dists).average().getAsDouble());
                    } else {
                        printoutReceivedNum("filtered by similarity in PW-DM", 0);
                    }
                } catch (Exception ex) {
                    logger.error("", ex);
                }
                return;
            }
            qtp = qtpQueue2Process.poll();
            if (qtp == null) {
                continue;
            }
            qid_tweetnum_received.adjustOrPutValue(qtp.queryid, 1, 1);
            try {
                // make decision until we have receive enough tweets for each query
                if (IntStream.of(qid_tweetnum_received.values()).average().getAsDouble() < Configuration.PW_DW_CUMULATECOUNT_DELAY) {
                    continue;
                }
            } catch (Exception ex) {
                logger.error("", ex);
                continue;
            }
            queryid = qtp.queryid;
            try {
                if (!qidNotificationNumTracker.containsKey(queryid)) {
                    qidNotificationNumTracker.put(queryid, 0);
                }
                if (qidNotificationNumTracker.get(queryid) < Configuration.PW_DM_SELECTNUM) {
                    if (scoreFilter(qtp)) {
                        double[] similarity;
                        synchronized (qidSentTweetQueues) {
                            similarity = compare2SentTweetFilger(qtp, qidSentTweetQueues);
                        }
                        if (similarity != null) {
                            CandidateTweet resultTweet = decisionMake(qtp, similarity, qidNotificationNumTracker);
                            if (resultTweet.rank > 0) {
                                try {
                                    // write down the tweets that are notified
                                    resultprinter.printResult(queryid, resultTweet.notificationOutput());
                                    resultprinter.printlog(queryid, qtp.getTweetText(), qtp.getUrlTitleText(), resultTweet.getAbsScore(), resultTweet.getRelScore());
                                } catch (FileNotFoundException | ParseException ex) {
                                    logger.error("", ex);
                                }
                                qidNotificationNumTracker.adjustOrPutValue(queryid, 1, 1);
                                //logger.info(qidNotificationNumTracker.get(queryid) + " " + resultTweet.toString() + " " + qtp.getStatus().getText() + " " + qtpQueue2Process.size());
                            } else {
                                //logger.info("qtp has not been selected: " + qtp.getRelScore() + "  " + qtp.getAbsScore() + " " + qidAbsThreshold2SentTweet.get(qtp.queryid));
                            }
                        } else {
                            num_filteredby_similarity.adjustOrPutValue(queryid, 1, 1);
                            //logger.info("filter out the qtp by distance: " + qidSentTweetQueues.get(qtp.queryid).size());
                        }
                    } else {
                        //logger.info("filter out the qtp by score: " + qtp.getRelScore());
                    }
                } else {
                    //logger.info("Finished: " + qtp.queryid + " " + qidNotificationNumTracker.get(qtp.queryid));
                    finishNotificationQidTracker.add(qtp.queryid);
                    if (finishNotificationQidTracker.size() >= queryRelativeScoreTracker.size()) {
                        logger.info("Finished all in PW-DM in this round: " + finishNotificationQidTracker.size());
                        clear();
                        break;
                    }
                }
            } catch (Exception ex) {
                logger.error("", ex);
            }
        }

    }

    private void clear() {
        resultprinter.flush();
        for (String qid : queryRelativeScoreTracker.keySet()) {
            queryRelativeScoreTracker.get(qid).ceasePWQueue();
        }
        if (qtpQueue2Process.size() > 0) {
            logger.warn(qtpQueue2Process.size() + " tweets will be cleared without processing in PW-DM");
            qtpQueue2Process.clear();
        }
    }

    /**
     * filter out the tweets that with low score
     *
     * @param tweet
     * @return
     */
    private boolean scoreFilter(QueryTweetPair tweet) {
        boolean isRetain = false;
        double relativeScore = tweet.getRelScore();
        if (relativeScore > Configuration.PW_DM_SCORE_FILTER) {
            isRetain = true;
        }
        return isRetain;
    }

    /**
     * "send the qtp" and keep tracking all the pop-up tweets in
     * qidSentTweetQueues meanwhile adjusting/maintaining the threshold for the
     * gain to make decision. Intuitively, a qtp will be pop-up as long as it is
     * highly relevant and divergent enough from previously notification. In
     * this decision maker, the gain is computed as the sum-product of the
     * absolute relevance and the relative distance. The absolute relevance is
     * from the predictor, e.g., the output probability for the point belonging
     * to class 1. And the relative distance is the distance between current
     * tweets and notified tweets w.r.t. the average distance among current
     * centroids. All other decision makers should override this method.
     *
     * @param tweet
     * @param similarities
     * @param queryNumberCount
     * @return
     */
    protected CandidateTweet decisionMake(QueryTweetPair tweet, double[] similarities, TObjectIntMap<String> queryNumberCount) {
        double absoluteScore = tweet.getAbsScore();
        String queryId = tweet.queryid;

        double avggain = 0;
        CandidateTweet resultTweet = new CandidateTweet(tweet);
        // the similarities w.r.t. all popped up tweets
        if (similarities.length > 0) {
            for (double similarity : similarities) {
                // similarity should be between 0 and 1
                similarity = Math.max(0, similarity);
                similarity = Math.min(1, similarity);
                avggain += (1 - similarity) * absoluteScore;
            }
            avggain /= (double) similarities.length;
            // if the average gain of the current qtp were larger than
            // the threshold, then pop-up the current qtp and lift the threshold
            // otherwise decrease the threshold
            if (adjustThreshold(queryId, avggain)) {
                resultTweet.rank = queryNumberCount.get(queryId) + 1;
            }
        } else {
            // if this is the first qtp to pop-up, we should make sure this is the 
            // qtp that have nearly highest relevance score since recent start, meanwhile we store this
            // relevance as initial absolute score threshold
            if (!qidInitRelativeThread2SentFirstTweet.containsKey(queryId)) {
                qidInitRelativeThread2SentFirstTweet.put(queryId, Configuration.PW_DM_FIRSTPOPUP_SCORETHRESD);
            }
            double currentThread = qidInitRelativeThread2SentFirstTweet.get(queryId);
            // get the updated relative score, this is expensive however
            double relativeScore = queryRelativeScoreTracker.get(queryId).relativeScore(absoluteScore);
            if (relativeScore > currentThread) {
                if (firstTweetDecider(tweet)) {
                    avggain = absoluteScore;
                    resultTweet.rank = queryNumberCount.get(queryId) + 1;
                    adjustThreshold(queryId, avggain);
                } else {
                    logger.info(tweet.getTweetText() + " for " + queryId + " didnt get thru our hand-code rules");
                }
            } else {
                currentThread *= (1 - Configuration.PW_DM_THRESHOLD_ALPHA);
                qidInitRelativeThread2SentFirstTweet.put(queryId, currentThread);
            }
        }
        // keep tracking all tweets being pop-up in the qidSentTweetQueues
        synchronized (qidSentTweetQueues) {
            updateSentTracker(resultTweet, qidSentTweetQueues, Configuration.PW_DM_SENT_QUEUETRACKER_LENLIMIT);
        }
        return resultTweet;
    }

    /**
     * some hand-code rules to decide whether to pop-up the first tweet for the
     * query, which actually set the threshold for all follow-up decision
     * making. Intuitively, if the tweet is long enough, with high retrieval
     * score, or it contains url with high similarity with the query (title), it
     * is very likely to be useful. Since we make decision for the first tweet
     * according to relative score, we dont want to have a too low bar.
     * Alternatively, we could pick up the tweets that contain all query terms,
     * which however will change our system design a little bit.
     *
     * @param tweet
     * @return
     */
    private boolean firstTweetDecider(QueryTweetPair tweet) {
        double tweetlength = 0;
        double urltitlescore = 0;
        boolean toSelect = false;
        try {
            tweetlength = tweet.getFeature(Configuration.FEATURE_T_LENGTH);
            urltitlescore = tweet.getFeature(Configuration.TWEET_URL_TITLE);
            // long enough, this is 2-3 setences
            if (tweetlength > 80) {
                toSelect = true;
                // high similarity between embedding url and query
            } else if (urltitlescore > 0.5) {
                toSelect = true;
                // at least one setence and include some relevant url tilte
            } else if (tweetlength > 40 && urltitlescore > 0.3) {
                toSelect = true;
            }
        } catch (Exception ex) {
            logger.error("", ex);
        }
        return toSelect;
    }

    /**
     * return true or false according to the comparison between current gain and
     * the existing threshold. If currentGain >= threshold, return true and lift
     * the threshold; if currentGain < threshold, return false and decrease the
     * threshold; it this is the first qtp for this query, i.e., the
     * qidAbsThreshold2SentTweet doesnt contain threshold, then return true and
     * store currentGain as threshold
     *
     * @param queryId
     * @param currentGain
     * @return
     */
    protected boolean adjustThreshold(String queryId, double currentGain) {
        boolean result;
        double threshold;
        if (qidAbsThreshold2SentTweet.containsKey(queryId)) {
            threshold = qidAbsThreshold2SentTweet.get(queryId);
            if (currentGain >= threshold) {
                result = true;
                // raise the threahold
                threshold *= (1 + Configuration.PW_DM_THRESHOLD_ALPHA);
            } else {
                result = false;
                // decrease the threahold
                threshold *= (1 - Configuration.PW_DM_THRESHOLD_ALPHA);
            }
        } else {
            result = true;
            threshold = currentGain;
        }
        qidAbsThreshold2SentTweet.put(queryId, threshold);
        return result;
    }

}
