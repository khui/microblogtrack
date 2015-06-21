package de.mpii.microblogtrack.utility;

import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import twitter4j.Status;

/**
 *
 * @author khui
 */
public class QueryTweetPair {

    private final TObjectDoubleMap<String> featureValues = new TObjectDoubleHashMap<>();

    private final TObjectDoubleMap<String> predictorResults = new TObjectDoubleHashMap<>();

    public final long tweetid;

    public final String queryid;

    private final Status status;

    public QueryTweetPair(long tweetid, String queryid, Status status) {
        this.tweetid = tweetid;
        this.queryid = queryid;
        this.status = status;
        generateTweetFeature();
        generateUserFeature();
    }

    public QueryTweetPair(QueryTweetPair qtp) {
        this.tweetid = qtp.tweetid;
        this.queryid = qtp.queryid;
        this.status = qtp.getStatus();
        this.featureValues.clear();
        this.predictorResults.clear();
        this.featureValues.putAll(qtp.getFeatures());
        this.predictorResults.putAll(qtp.getPredictRes());
    }

    /**
     * #retweet #like
     */
    private void generateTweetFeature() {
        for (String featurename : MYConstants.irModels) {
            featureValues.put(featurename, 0);
        }
    }

    /**
     * #retweet #following #follower
     */
    private void generateUserFeature() {

    }

    public void updateFeatures(String name, double score) {
        featureValues.put(name, score);
    }

    public TObjectDoubleMap<String> getFeatures() {
        return featureValues;
    }

    public TObjectDoubleMap<String> getPredictRes() {
        return predictorResults;
    }

    public double getFeature(String name) {
        return featureValues.get(name);
    }

    public Status getStatus() {
        return this.status;
    }

    public void setPredictScore(String predictresultname, double predictscore) {
        this.predictorResults.put(predictresultname, predictscore);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(queryid).append(":").append(tweetid).append(" ");
        for (String featurename : MYConstants.irModels) {
            sb.append(featurename).append(":").append(featureValues.get(featurename)).append(" ");
        }
        return sb.toString();
    }

}
