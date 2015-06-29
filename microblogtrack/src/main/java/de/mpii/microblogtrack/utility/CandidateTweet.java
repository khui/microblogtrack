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
    public final double relativeScore;
    // absolute score
    public final double absoluteScore;
    public double distance = -1;

    public boolean isSelected = false;

    private final Vector featureVector;

    public CandidateTweet(long tweetid, double absoluteS, double prob, boolean selected, String queryId, Vector featureVector) {
        this.tweetid = tweetid;
        this.featureVector = featureVector;
        this.relativeScore = prob;
        this.queryId = queryId;
        this.absoluteScore = absoluteS;
        this.isSelected = selected;
    }

    public CandidateTweet(CandidateTweet candidatetweet) {
        this.tweetid = candidatetweet.tweetid;
        this.featureVector = candidatetweet.getFeature();
        this.relativeScore = candidatetweet.relativeScore;
        this.queryId = candidatetweet.queryId;
        this.distance = candidatetweet.distance;
        this.absoluteScore = candidatetweet.absoluteScore;
        this.isSelected = candidatetweet.isSelected;
    }

    public void setDist(double dist) {
        this.distance = dist;
    }

    public Vector getFeature() {
        return this.featureVector;
    }
}
