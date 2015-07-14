package de.mpii.microblogtrack.utility;

import java.text.DecimalFormat;
import org.apache.log4j.Logger;

/**
 * the unique tweet for the final results. Include the pointwise score and its
 * distance to one of the centroid and its feature vector.
 *
 * @author khui
 */
public class CandidateTweet extends QueryTweetPair {

    static Logger logger = Logger.getLogger(CandidateTweet.class.getName());

    public double distance = -1;

    public int rank = -1;
    // the time stamp when this tweets are sent
    public long sentTimeStamp = 0;
    // count how many tweets are similar to this tweet after it being sent; 
    public volatile int duplicateCount = 0;

    public CandidateTweet(QueryTweetPair tweet, int rank, long timestamp) {
        super(tweet);
        this.rank = rank;
        this.sentTimeStamp = timestamp;
    }

    public CandidateTweet(QueryTweetPair tweet, int rank) {
        this(tweet, rank, 0);
    }

    public CandidateTweet(QueryTweetPair tweet) {
        this(tweet, -1, 0);
    }

    public CandidateTweet(CandidateTweet candidatetweet, long timestamp) {
        super(candidatetweet);
        this.sentTimeStamp = timestamp;
        this.rank = candidatetweet.rank;
    }

    public CandidateTweet(CandidateTweet candidatetweet) {
        this(candidatetweet, candidatetweet.sentTimeStamp);
    }

    public void setDist(double dist) {
        this.distance = dist;
    }

    public String toTrecFormat() {
        StringBuilder sb = new StringBuilder();
        sb.append(queryid).append(" ");
        sb.append("Q0").append(" ");
        sb.append(tweetid).append(" ");
        sb.append(rank).append(" ");
        sb.append(Configuration.RES_RUNINFO);
        //sb.append(tweetstr);
        return sb.toString();
    }

    public String forDebugToString(String tweetstr) {
        DecimalFormat df = new DecimalFormat(".###");
        StringBuilder sb = new StringBuilder();
        sb.append(queryid).append("\t");
        sb.append(tweetid).append("\t");
        sb.append(rank).append("\t");
        sb.append(df.format(getRelScore())).append("\t");
        sb.append(df.format(getAbsScore())).append("\t");
        sb.append(tweetstr);
        return sb.toString();
    }

}
