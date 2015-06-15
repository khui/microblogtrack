package de.mpii.microblogtrack.component.filter;

import gnu.trove.TCollections;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import java.util.Arrays;
import twitter4j.JSONObject;
import twitter4j.Status;

/**
 *
 * @author khui
 */
public class DuplicateTweet implements Filter {

    private static final TLongSet tweetids_0 = TCollections.synchronizedSet(new TLongHashSet());

    private static final TLongSet tweetids_1 = new TLongHashSet();

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
        synchronized (tweetids_0) {
            if (!tweetids_0.contains(tweetid) && !tweetids_1.contains(tweetid)) {
                tweetids_0.add(tweetid);
            } else {
                isNew = false;
            }
        }
        return isNew;
    }

    public void clearIdSet() {
        synchronized (tweetids_0) {
            tweetids_1.clear();
            tweetids_1.addAll(tweetids_0);
            tweetids_0.clear();
        }
    }

    public long[] getTweetIdRange() {
        long maxid;
        long minid;
        long[] ids;
        synchronized (tweetids_0) {
            ids = tweetids_0.toArray();
            tweetids_1.clear();
            tweetids_1.addAll(tweetids_0);
            tweetids_0.clear();
        }
        Arrays.sort(ids);
        minid = ids[0];
        maxid = ids[ids.length - 1];
        return new long[]{minid, maxid};
    }

}
