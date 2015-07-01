package de.mpii.microblogtrack.utility;

import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import java.util.Arrays;
import org.apache.log4j.Logger;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import twitter4j.Status;

/**
 *
 * @author khui
 */
public class QueryTweetPair {

    static Logger logger = Logger.getLogger(QueryTweetPair.class.getName());

    private final TObjectDoubleMap<String> featureValues = new TObjectDoubleHashMap<>();

    private final TObjectDoubleMap<String> predictorResults = new TObjectDoubleHashMap<>();

    private static String[] featureNames = null;

    public final long tweetid;

    public final String queryid;

    private final Status status;

    private Vector featureVector = null;

    public QueryTweetPair(long tweetid, String queryid, Status status) {
        this.tweetid = tweetid;
        this.queryid = queryid;
        this.status = status;
        semanticMatchFeatures();
        tweetFeatures();
        userFeatures();
    }

    public QueryTweetPair(QueryTweetPair qtp) {
        this.tweetid = qtp.tweetid;
        this.queryid = qtp.queryid;
        this.status = qtp.getStatus();
        this.featureValues.clear();
        this.predictorResults.clear();
        this.featureValues.putAll(qtp.getFeatures());
        this.predictorResults.putAll(qtp.getPredictRes());
        this.featureVector = qtp.vectorize();
    }

    public void updateFeatures(String name, double score) {
        featureValues.put(name, score);
    }

    public TObjectDoubleMap<String> getPredictRes() {
        return predictorResults;
    }

    public double getFeature(String name) {
        return featureValues.get(name);
    }

    public void setPredictScore(String predictresultname, double predictscore) {
        this.predictorResults.put(predictresultname, predictscore);
    }

    public double getAbsScore() {
        double score = -1;
        if (predictorResults.containsKey(MYConstants.PRED_ABSOLUTESCORE)) {
            score = predictorResults.get(MYConstants.PRED_ABSOLUTESCORE);
        }
        return score;
    }

    public double getRelScore() {
        double score = -1;
        if (predictorResults.containsKey(MYConstants.PRED_RELATIVESCORE)) {
            score = predictorResults.get(MYConstants.PRED_RELATIVESCORE);
        }
        return score;
    }

    /**
     * for debug to printQueryTweet
     *
     * @return
     */
    public Status getStatus() {
        return this.status;
    }

    public Vector vectorize() {
        if (featureVector != null) {
            return featureVector;
        }
        boolean regenerateFeatureNames = false;
        if (featureNames == null) {
            regenerateFeatureNames = true;
        } else if (featureNames.length != featureValues.size()) {
            regenerateFeatureNames = true;
        }
        if (regenerateFeatureNames) {
            featureNames = featureValues.keys(new String[0]);
            Arrays.sort(featureNames);
        }
        double[] fvalues = new double[featureNames.length];
        for (int i = 0; i < fvalues.length; i++) {
            fvalues[i] = featureValues.get(featureNames[i]);
        }
        featureVector = new DenseVector(fvalues);
        return featureVector;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(queryid).append(":").append(tweetid).append(" ");
//        for (String featurename : MYConstants.irModels) {
//            sb.append(featurename).append(":").append(featureValues.get(featurename)).append(" ");
//        }
        sb.append(getAbsScore()).append(" ");
        sb.append(getRelScore());
        return sb.toString();
    }

    /**
     * semantic matching features
     */
    private void semanticMatchFeatures() {
        for (String featurename : MYConstants.irModels) {
            featureValues.put(featurename, 0);
        }
    }

    /**
     * #retweet #like #hashmap url
     */
    private void tweetFeatures() {

    }

    /**
     * #retweet #following #follower
     */
    private void userFeatures() {

    }

    private TObjectDoubleMap<String> getFeatures() {
        return featureValues;
    }

}
