package de.mpii.microblogtrack.component.core;

import de.mpii.lowcosteval.maxrep.MaxReponSimilarity;
import de.mpii.microblogtrack.utility.CandidateTweet;
import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import de.mpii.microblogtrack.utility.io.printresult.ResultPrinter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.log4j.Logger;

/**
 * collect tweets from lucene, thereafter return top-100 list at the end of day.
 * in particular, we keep track the first k documents with highest score since
 * today, at the end of each day we select top-100 tweets to generate output for
 * e-mail digest
 *
 * @author khui
 */
public class ListwiseDecisionMakerMapRep extends ListwiseDecisionMaker {
    
    static Logger logger = Logger.getLogger(ListwiseDecisionMakerMapRep.class.getName());
    
    public ListwiseDecisionMakerMapRep(Map<String, LuceneDMConnector> tracker, BlockingQueue<QueryTweetPair> tweetqueue, ResultPrinter resultprinter) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        super(tracker, tweetqueue, resultprinter);
        logger.info(ListwiseDecisionMakerMapRep.class.getName() + " is being used.");
    }
    
    @Override
    protected List<CandidateTweet> decisionMake(PriorityBlockingQueue<QueryTweetPair> queue, String qid) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        List<QueryTweetPair> candidateTweets = new ArrayList<>();
        int tweetnum = queue.drainTo(candidateTweets);
        if (tweetnum <= 0) {
            logger.error("The candidate tweet list is empty");
            return null;
        }
        for (QueryTweetPair qtp : candidateTweets) {
            qtp.setPredictScore(Configuration.PRED_RELATIVESCORE, queryRelativeScoreTracker.get(qid).relativeScore(qtp.getAbsScore()));
        }
        MaxReponSimilarity selector = new MaxReponSimilarity(candidateTweets);
        int[] selectedIndex = selector.selectMaxRep(Configuration.LW_DM_SELECTNUM);
        List<CandidateTweet> selectedQTPs = new ArrayList<>();
        int rank = 1;
        for (int index : selectedIndex) {
            selectedQTPs.add(new CandidateTweet(candidateTweets.get(index), rank++));
        }
        // decreasing order
        selectedQTPs.sort((CandidateTweet o1, CandidateTweet o2) -> {
            if (o1.getAbsScore() > o2.getAbsScore()) {
                return -1;
            } else if (o1.getAbsScore() < o2.getAbsScore()) {
                return 1;
            } else if (o1.tweetid > o2.tweetid) {
                return -1;
            } else {
                return 1;
            }
        });
        for (CandidateTweet resultTweet : selectedQTPs) {
            synchronized (qidSentTweetQueues) {
                updateSentTracker(resultTweet, qidSentTweetQueues, Configuration.LW_DM_SENT_QUEUETRACKER_LENLIMIT);
            }
        }
        return selectedQTPs;
    }
    
}
