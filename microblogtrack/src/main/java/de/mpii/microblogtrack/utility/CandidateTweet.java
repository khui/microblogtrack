package de.mpii.microblogtrack.utility;

import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import libsvm.svm_node;
import org.apache.log4j.Logger;

/**
 * the unique tweet for the final results. Include the pointwise score and its
 * distance to one of the centroid and its feature vector.
 *
 * @author khui
 */
public class CandidateTweet {

    static Logger logger = Logger.getLogger(CandidateTweet.class.getName());

    public final String queryId;
    public final long tweetId;
    // relative score
    public final double relativeScore;
    // absolute score
    public final double absoluteScore;
    public double distance = -1;

    public int rank = -1;
    // count how many tweets are similar to this tweet after it being sent; 
    public volatile int duplicateCount = 0;
    // the time stamp when this tweets are sent
    public long sentTimeStamp = 0;

    private final TObjectDoubleMap<String> featureValues = new TObjectDoubleHashMap<>();

    private final Map<String, String> contentString = new HashMap<>();

    private final svm_node[] vectorLibsvm;

    public CandidateTweet(QueryTweetPair tweet, int rank, long timestamp) {
        this.tweetId = tweet.tweetid;
        this.relativeScore = tweet.getRelScore();
        this.queryId = tweet.queryid;
        this.absoluteScore = tweet.getAbsScore();
        this.rank = rank;
        this.sentTimeStamp = timestamp;
        this.vectorLibsvm = tweet.vectorizeLibsvm();
        featureValues.putAll(tweet.getFeatures());
        contentString.putAll(tweet.getContentStr());
    }

    public CandidateTweet(QueryTweetPair tweet) {
        this(tweet, -1, 0);
    }

    public CandidateTweet(CandidateTweet candidatetweet) {
        this.tweetId = candidatetweet.tweetId;
        this.relativeScore = candidatetweet.relativeScore;
        this.queryId = candidatetweet.queryId;
        this.absoluteScore = candidatetweet.absoluteScore;
        this.rank = candidatetweet.rank;
        this.sentTimeStamp = candidatetweet.sentTimeStamp;
        this.featureValues.clear();
        this.featureValues.putAll(candidatetweet.featureValues);
        this.contentString.clear();
        this.contentString.putAll(candidatetweet.contentString);
        this.vectorLibsvm = candidatetweet.vectorLibsvm;
    }

    public String getTweetText() {
        return contentString.get(Configuration.TWEET_CONTENT);
    }

    public String getUrlTitleText() {
        if (contentString.containsKey(Configuration.TWEET_URL_TITLE)) {
            return contentString.get(Configuration.TWEET_URL_TITLE);
        } else {
            return null;
        }
    }

    public CandidateTweet(CandidateTweet candidatetweet, long timestamp) {
        this(candidatetweet);
        this.sentTimeStamp = timestamp;
    }

    public void setDist(double dist) {
        this.distance = dist;
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
        DecimalFormat df = new DecimalFormat(".###");
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
