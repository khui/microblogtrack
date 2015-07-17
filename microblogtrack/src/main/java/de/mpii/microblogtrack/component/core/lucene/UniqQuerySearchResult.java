package de.mpii.microblogtrack.component.core.lucene;

import de.mpii.microblogtrack.utility.QueryTweetPair;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

/**
 *
 * @author khui
 */
public class UniqQuerySearchResult {

    static Logger logger = Logger.getLogger(UniqQuerySearchResult.class.getName());

    public final String queryid;
    private final Collection<QueryTweetPair> results;

    public UniqQuerySearchResult(String queryid, Collection<QueryTweetPair> results) {
        this.queryid = queryid;
        this.results = results;
    }

    public Collection<QueryTweetPair> getSearchResults() {
        return results;
    }

    public void offer2queue(BlockingQueue<QueryTweetPair> queue2offer4LW) throws InterruptedException {
        for (QueryTweetPair qtp : results) {
            // offer to the blocking queue for the decision maker
            boolean isSucceed = queue2offer4LW.offer(new QueryTweetPair(qtp), 1000, TimeUnit.MILLISECONDS);
            if (!isSucceed) {
                logger.error("offer to queue2offer4LW failed: " + queue2offer4LW.size());
            }
        }
    }

    public void offer2queue(BlockingQueue<QueryTweetPair> queue2offer4PW, BlockingQueue<QueryTweetPair> queue2offer4LW) throws InterruptedException {
        for (QueryTweetPair qtp : results) {
            // offer to the blocking queue for the pointwise decision maker
            boolean isSucceed = queue2offer4PW.offer(new QueryTweetPair(qtp), 1000, TimeUnit.MILLISECONDS);
            if (!isSucceed) {
                logger.error("offer to queue2offer4PW failed: " + queue2offer4PW.size());
            }
            // offer to the blocking queue for the decision maker
            isSucceed = queue2offer4LW.offer(new QueryTweetPair(qtp), 1000, TimeUnit.MILLISECONDS);
            if (!isSucceed) {
                logger.error("offer to queue2offer4LW failed: " + queue2offer4LW.size());
            }
        }
    }

}
