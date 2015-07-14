package de.mpii.microblogtrack.component;

import gnu.trove.TCollections;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import org.apache.log4j.Logger;
import twitter4j.JSONObject;
import twitter4j.Status;

/**
 *
 * @author khui
 */
public class IndexTracker {

    final Logger logger = Logger.getLogger(IndexTracker.class);

    private static final TLongObjectMap<StatusCount> tweetidStatus_0 = TCollections.synchronizedMap(new TLongObjectHashMap<StatusCount>(10000));

    private static final TLongObjectMap<StatusCount> tweetidStatus_1 = TCollections.synchronizedMap(new TLongObjectHashMap<StatusCount>(10000));
    /**
     * each time we roughly process tweets received in last minutes. to
     * accurately achieve that, we use these synchronized tweet counts as id,
     * whose variable is obviously monotonic increasing. we use twittercount and
     * lastreportedcount as id boundary in each search, by generating a numeric
     * range query
     *
     */
    // track the number of twitter we have indexed
    private long twittercount = 0;
    // twitter tweetCountId last time reported
    private long lastreportedcount = 0;

    /**
     * keep tracking the latest tweets to check the duplicate tweets since no
     * suitable queue is found, here I use two synchronizedSet to do the job
     *
     * @param msg
     * @param json
     * @param status
     * @return
     */
    public long isDuplicate(String msg, JSONObject json, Status status) {
        long tweetid = status.getId();
        long tweetcountId;
        synchronized (tweetidStatus_0) {
            if (!tweetidStatus_0.containsKey(tweetid) && !tweetidStatus_1.containsKey(tweetid)) {
                twittercount++;
                tweetidStatus_0.put(tweetid, new StatusCount(twittercount, status));
                tweetcountId = twittercount;
            } else {
                tweetcountId = -1;
            }
        }
        return tweetcountId;
    }

    public long[] minMaxTweetCountInTimeInterval() {
        long[] minmax = new long[2];
        synchronized (tweetidStatus_0) {
            // twitter tweetCountId to report
            minmax[1] = twittercount + 1;
            // twitter tweetCountId that last time reported
            minmax[0] = lastreportedcount;
            lastreportedcount = minmax[1];
            tweetidStatus_1.clear();
            tweetidStatus_1.putAll(tweetidStatus_0);
            tweetidStatus_0.clear();
        }
        return minmax;
    }

    public long getRawTweetCount() {
        return twittercount;
    }

    public Status getStatus(long tweetid) {
        if (tweetidStatus_1.containsKey(tweetid)) {
            return tweetidStatus_1.get(tweetid).getStatus();
        } else {
            logger.error("the map doesnot contain tweetid: " + tweetid);
            return null;
        }
    }

    public long getStautsCount(long tweetid) {
        if (tweetidStatus_1.containsKey(tweetid)) {
            return tweetidStatus_1.get(tweetid).tweetCountId;
        } else {
            logger.error("the map doesnot contain tweetid: " + tweetid);
            return -1;
        }
    }

    private class StatusCount {

        protected final long tweetCountId;
        private final Status status;

        protected StatusCount(long tweetCountId, Status status) {
            this.tweetCountId = tweetCountId;
            this.status = status;
        }

        protected Status getStatus() {
            return status;
        }
    }

}
