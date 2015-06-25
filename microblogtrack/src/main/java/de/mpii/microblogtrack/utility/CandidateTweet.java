package de.mpii.microblogtrack.utility;

import org.apache.mahout.math.Vector;

/**
 * the unique tweet for the final results. Include the pointwise score and its
 * distance to one of the centroid and its feature vector.
 *
 * @author khui
 */
public class CandidateTweet {

    public final String queryId;
    public final long tweetid;
    // relative score
    public final double queryIndependentScore;
    // absolute score
    public final double queryDependentScore;
    public double distance = -1;
    // to distinguish different centroids in unique run, is not universial id
    private int centroidid = 0;
    private final Vector featureVector;

    public CandidateTweet(long tweetid, double score, double prob, String queryId, Vector featureVector) {
        this.tweetid = tweetid;
        this.featureVector = featureVector;
        this.queryIndependentScore = prob;
        this.queryId = queryId;
        this.queryDependentScore = score;
    }

    public CandidateTweet(long tweetid, double score, double prob, double dist, int centroidId, String queryId, Vector featureVector) {
        this.tweetid = tweetid;
        this.featureVector = featureVector;
        this.queryIndependentScore = prob;
        this.queryId = queryId;
        this.centroidid = centroidId;
        this.distance = dist;
        this.queryDependentScore = score;
    }

    public CandidateTweet(CandidateTweet candidatetweet) {
        this.tweetid = candidatetweet.tweetid;
        this.featureVector = candidatetweet.getFeature();
        this.queryIndependentScore = candidatetweet.queryIndependentScore;
        this.queryId = candidatetweet.queryId;
        this.centroidid = candidatetweet.getCentroidId();
        this.distance = candidatetweet.distance;
        this.queryDependentScore = candidatetweet.queryDependentScore;
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
