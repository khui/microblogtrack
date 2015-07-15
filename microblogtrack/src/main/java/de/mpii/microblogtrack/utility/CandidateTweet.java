package de.mpii.microblogtrack.utility;

import java.text.DecimalFormat;
import java.text.ParseException;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

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
    private String utc_send_timestamp = null;

    public long millis_send_timestamp = 0;
    // count how many tweets are similar to this tweet after it being sent; 
    public volatile int duplicateCount = 0;

    private final DecimalFormat df = new DecimalFormat("#.#####");

    private CandidateTweet(QueryTweetPair tweet, int rank, String timestamp, long ltimestamp) {
        super(tweet);
        this.rank = rank;
        this.utc_send_timestamp = timestamp;
        this.millis_send_timestamp = ltimestamp;
    }

    public CandidateTweet(QueryTweetPair tweet, int rank) {
        this(tweet, rank, null, 0);
    }

    public CandidateTweet(QueryTweetPair tweet) {
        this(tweet, -1, null, 0);
    }

    private CandidateTweet(CandidateTweet candidatetweet, String timestamp, long ltimestamp) {
        super(candidatetweet);
        this.utc_send_timestamp = timestamp;
        this.rank = candidatetweet.rank;
        this.millis_send_timestamp = ltimestamp;
    }

    public CandidateTweet(CandidateTweet candidatetweet) {
        this(candidatetweet, candidatetweet.utc_send_timestamp, candidatetweet.millis_send_timestamp);
    }

    public void setDist(double dist) {
        this.distance = dist;
    }

    public void setTimeStamp() {
        DateTime nowUtc = DateTime.now(DateTimeZone.UTC);
        DateTimeFormatter dfjoda = DateTimeFormat.forPattern("EEEE, MMMM dd, Y, HH:mm:ss z").withZoneUTC();
        //ISODateTimeFormat.dateTime().withZoneUTC();
        this.millis_send_timestamp = nowUtc.getMillis();
        this.utc_send_timestamp = dfjoda.print(nowUtc);
    }

    public String notificationOutput() throws ParseException {
        StringBuilder sb = new StringBuilder();
        sb.append(queryid).append(" ");
        sb.append("Q0").append(" ");
        sb.append(tweetid).append(" ");
        sb.append(rank).append(" ");
        sb.append(df.format(getAbsScore())).append(" ");
        sb.append(utc_send_timestamp).append(" ");
        sb.append(Configuration.RUN_ID);
        return sb.toString();
    }

    public String digestOutput(int rank) throws ParseException {
        StringBuilder sb = new StringBuilder();
        sb.append(queryid).append(" ");
        sb.append("Q0").append(" ");
        sb.append(tweetid).append(" ");
        sb.append(rank).append(" ");
        sb.append(df.format(getAbsScore())).append(" ");
        sb.append(Configuration.RUN_ID);
        return sb.toString();
    }

    public String logString(String tweetstr) {
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
