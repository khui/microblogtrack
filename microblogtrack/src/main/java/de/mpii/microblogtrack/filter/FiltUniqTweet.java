package de.mpii.microblogtrack.filter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import de.mpii.microblogtrack.utility.UniqTweet;
import org.apache.log4j.Logger;
import twitter4j.JSONObject;
import twitter4j.Status;

/**
 *
 * @author khui
 */
public class FiltUniqTweet implements Callable<UniqTweet> {

    final Logger logger = Logger.getLogger(FiltUniqTweet.class);

    private final BlockingQueue<String> rawMsgQueue;

    private final Filter[] filters;

    private int TIMEOUT = 60;

    public FiltUniqTweet(BlockingQueue<String> rawMsgQueue, Filter[] filters, int timeout) {
        this.rawMsgQueue = rawMsgQueue;
        this.filters = filters;
        this.TIMEOUT = timeout;
    }

    /**
     *
     * @return @throws InterruptedException
     */
    @Override
    public UniqTweet call() throws InterruptedException {
        JSONObject json = null;
        Status tweet = null;
        String msg = null;
        while (msg == null) {
            msg = rawMsgQueue.poll(TIMEOUT, TimeUnit.SECONDS);
            if (msg == null) {
                logger.error("Get no msg within past " + TIMEOUT + " seconds");
            }
        }
        // always firstly check whether the stream is a tweet 
        StatusFilter statusfilter = (StatusFilter) filters[0];
        boolean isstatus = statusfilter.isRetain(msg, null, null);
        boolean isretained = false;
        UniqTweet uniqtweet = null;
        if (!isstatus) {
            uniqtweet = new UniqTweet(null, null, msg);
        } else {
            json = statusfilter.getJson();
            tweet = statusfilter.getStatus();
            uniqtweet = new UniqTweet(tweet, json, msg);
            for (int i = 1; i < filters.length; i++) {
                isretained = filters[i].isRetain(msg, json, tweet);
                if (!isretained) {
                    break;
                }
            }
        }
        uniqtweet.setIsRetained(isretained);
        return uniqtweet;
    }
}
