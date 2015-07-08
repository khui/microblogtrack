package de.mpii.microblogtrack.utility;

import java.text.DecimalFormat;
import org.apache.mahout.math.Vector;

/**
 * the unique tweet for the final results. Include the pointwise score and its
 * distance to one of the centroid and its feature vector.
 *
 * @author khui
 */
public class CandidateTweet {

    public final String queryId;
    public final long tweetId;
    // relative score
    public final double relativeScore;
    // absolute score
    public final double absoluteScore;
    public double distance = -1;

    public int rank = -1;

    private final Vector featureVector;

    public CandidateTweet(long tweetid, double absoluteS, double prob, int rank, String queryId, Vector featureVector) {
        this.tweetId = tweetid;
        this.featureVector = featureVector;
        this.relativeScore = prob;
        this.queryId = queryId;
        this.absoluteScore = absoluteS;
        this.rank = rank;
    }

    public CandidateTweet(long tweetid, double absoluteS, double prob, String queryId, Vector featureVector) {
        this.tweetId = tweetid;
        this.featureVector = featureVector;
        this.relativeScore = prob;
        this.queryId = queryId;
        this.absoluteScore = absoluteS;
    }

    public CandidateTweet(CandidateTweet candidatetweet) {
        this.tweetId = candidatetweet.tweetId;
        this.featureVector = candidatetweet.getFeature();
        this.relativeScore = candidatetweet.relativeScore;
        this.queryId = candidatetweet.queryId;
        this.distance = candidatetweet.distance;
        this.absoluteScore = candidatetweet.absoluteScore;
        this.rank = candidatetweet.rank;
    }

    public void setDist(double dist) {
        this.distance = dist;
    }

    public Vector getFeature() {
        return this.featureVector;
    }

    public String toTrecFormat() {
        StringBuilder sb = new StringBuilder();
        sb.append(queryId).append(" ");
        sb.append("Q0").append(" ");
        sb.append(tweetId).append(" ");
        sb.append(rank).append(" ");
        sb.append(Configuration.RES_RUNINFO);
        //sb.append(tweetstr);
        return sb.toString();
    }

    public String forDebugToString(String tweetstr) {
        DecimalFormat df = new DecimalFormat("#.###");
        StringBuilder sb = new StringBuilder();
        sb.append(queryId).append("\t");
        sb.append(tweetId).append("\t");
        sb.append(rank).append("\t");
        sb.append(df.format(relativeScore)).append("\t");
        sb.append(df.format(absoluteScore)).append("\t");
        sb.append(tweetstr);
        return sb.toString();
    }

}
