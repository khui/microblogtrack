package de.mpii.microblogtrack.utility;

import twitter4j.JSONObject;
import twitter4j.Status;

/**
 * the different format of one tweet, representing different form of a tweet in
 * different processing phase
 *
 * @author khui
 */
public class UniqTweet {

    private Status tweet = null;

    private JSONObject json = null;

    private String rawString = null;

    private long tweetid = -1;

    public boolean isStatus = false;
    /**
     * by default, the stream is discarded
     */
    public boolean isRetained = false;

    public UniqTweet(Status tweet, JSONObject json, String rawString) {
        this.tweet = tweet;
        this.json = json;
        this.rawString = rawString;
        if (tweet != null) {
            isStatus = true;
            tweetid = tweet.getId();
        }
    }

    public void setIsRetained(boolean isRetained) {
        this.isRetained = isRetained;
    }

    public Status getStatus() {
        return this.tweet;
    }

    public JSONObject getJson() {
        return this.json;
    }

    public String getMsg() {
        return this.rawString;
    }

    public long getTweetId() {
        return tweetid;
    }

}
