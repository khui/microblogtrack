package de.mpii.microblogtrack.component.filter;

import gnu.trove.TCollections;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.util.Arrays;
import org.apache.log4j.Logger;
import twitter4j.JSONObject;
import twitter4j.Status;

/**
 *
 * @author khui
 */
public class DuplicateTweet implements Filter {

    final Logger logger = Logger.getLogger(DuplicateTweet.class);

    private static final TLongObjectMap<Status> tweetidStatus_0 = TCollections.synchronizedMap(new TLongObjectHashMap<Status>(10000));

    private static final TLongObjectMap<Status> tweetidStatus_1 = TCollections.synchronizedMap(new TLongObjectHashMap<Status>(10000));

    private final int tracklength = 10000;

    /**
     * keep tracking the latest tracklength tweets to check the duplicate tweets
     * since no suitable queue is found, here I use two synchronizedSet to do
     * the job
     *
     * @param msg
     * @param json
     * @param status
     * @return
     */
    @Override
    public boolean isRetain(String msg, JSONObject json, Status status) {
        long tweetid = status.getId();
        boolean isNew = true;
        synchronized (tweetidStatus_0) {
            if (!tweetidStatus_0.containsKey(tweetid) && !tweetidStatus_1.containsKey(tweetid)) {
                tweetidStatus_0.put(tweetid, status);
            } else {
                isNew = false;
            }
        }
        return isNew;
    }

    public long[] getTweetIdRange() {
        long maxid;
        long minid;
        long[] ids;

        synchronized (tweetidStatus_0) {
            ids = tweetidStatus_0.keys();
            tweetidStatus_1.clear();
            tweetidStatus_1.putAll(tweetidStatus_0);
            tweetidStatus_0.clear();
        }
        Arrays.sort(ids);
        minid = ids[0];
        maxid = ids[ids.length - 1];
        return new long[]{minid, maxid};
    }

    public Status getStatus(long tweetid) {
        if (tweetidStatus_1.containsKey(tweetid)) {
            return tweetidStatus_1.get(tweetid);
        } else {
            logger.error("the map doesnot contain tweetid: " + tweetid);
            return null;
        }
    }

}
