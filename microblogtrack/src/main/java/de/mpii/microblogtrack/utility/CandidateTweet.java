package de.mpii.microblogtrack.utility;

import org.apache.mahout.math.Vector;

/**
 *
 * @author khui
 */
public class CandidateTweet {

    public final String queryId;
    public long tweetid;
    public double prob;
    public double distance = -1;
    // to distinguish different centroids in unique run
    private int centroidid = 0;
    private final Vector featureVector;

    public CandidateTweet(long tweetid, double prob, String queryId, Vector featureVector) {
        this.tweetid = tweetid;
        this.featureVector = featureVector;
        this.prob = prob;
        this.queryId = queryId;
    }

    public CandidateTweet(long tweetid, double prob, double dist, int centroidId, String queryId, Vector featureVector) {
        this.tweetid = tweetid;
        this.featureVector = featureVector;
        this.prob = prob;
        this.queryId = queryId;
        this.centroidid = centroidId;
        this.distance = dist;
    }

    public CandidateTweet(CandidateTweet candidatetweet) {
        this.tweetid = candidatetweet.tweetid;
        this.featureVector = candidatetweet.getFeature();
        this.prob = candidatetweet.prob;
        this.queryId = candidatetweet.queryId;
        this.centroidid = candidatetweet.getCentroidId();
        this.distance = candidatetweet.distance;
    }

    public void setCentroidId(int centroidid) {
        this.centroidid = centroidid;
    }

    public void setDist(double dist) {
        this.distance = dist;
    }

    public int getCentroidId() {
        return this.centroidid;
    }

    public Vector getFeature() {
        return this.featureVector;
    }
}
