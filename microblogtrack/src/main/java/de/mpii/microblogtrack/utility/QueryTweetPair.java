package de.mpii.microblogtrack.utility;

import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import java.util.Arrays;
import java.util.Map;
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

    protected Status status;

    private Vector featureVector = null;

    public QueryTweetPair(long tweetid, String queryid, Status status) {
        this.tweetid = tweetid;
        this.queryid = queryid;
        this.status = status;
        updateFeatures();
    }

    public QueryTweetPair(long tweetid, String queryid) {
        this(tweetid, queryid, null);
    }

    public QueryTweetPair(QueryTweetPair qtp) {
        this.tweetid = qtp.tweetid;
        this.queryid = qtp.queryid;
        this.status = qtp.getStatus();
        this.featureValues.clear();
        this.predictorResults.clear();
        this.featureValues.putAll(qtp.getFeatures());
        this.predictorResults.putAll(qtp.getPredictRes());
        this.featureVector = qtp.vectorizeMahout();
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

    public TObjectDoubleMap<String> getFeatures() {
        return featureValues;
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

    public Vector vectorizeMahout() {
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
//        for (String featurename : MYConstants.FEATURES_SEMANTIC) {
//            sb.append(featurename).append(":").append(featureValues.get(featurename)).append(" ");
//        }
        sb.append(getAbsScore()).append(" ");
        sb.append(getRelScore());
        return sb.toString();
    }

    /**
     * we use min/max scaling method, rescale each feature to [lower, upper]
     * interval, afterward the vector representations are rebuilt. The min/max
     * value for each feature can either come from off-line computation or
     * online tracking
     *
     * @param featureMinMax
     * @param lower
     * @param upper
     */
    public void rescaleFeatures(Map<String, double[]> featureMinMax, double lower, double upper) {
        double max, min, r_value, n_value;
        String[] features = featureValues.keySet().toArray(new String[0]);
        for (String feature : features) {
            if (featureMinMax.containsKey(feature)) {
                r_value = featureValues.get(feature);
                min = featureMinMax.get(feature)[0];
                max = featureMinMax.get(feature)[1];
                // due to the concurrency reason, the value might be outside 
                // [min, max]
                if (r_value <= min) {
                    n_value = lower;
                } else if (r_value >= max) {
                    n_value = upper;
                } else {
                    n_value = lower + (upper - lower)
                            * (r_value - min) / (max - min);
                }
                featureValues.put(feature, n_value);
            }
        }
        if (featureVector != null) {
            featureVector = null;
            vectorizeMahout();
        }
    }

    /**
     * rescale the feature to [0, 1]
     *
     * @param featureMinMax
     */
    public void rescaleFeatures(Map<String, double[]> featureMinMax) {
        rescaleFeatures(featureMinMax, 0.0, 1.0);
    }

    protected final void updateFeatures() {
        semanticFeatures();
        expansionFeatures();
        tweetFeatures();
        userFeatures();
    }

    /**
     * semantic matching features
     */
    private void semanticFeatures() {
        for (String featurename : MYConstants.FEATURES_SEMANTIC) {
            if (!featureValues.containsKey(featurename)) {
                featureValues.put(featurename, 0);
            }
        }
    }

    private void expansionFeatures() {
        for (String featurename : MYConstants.FEATURES_EXPANSION) {
            if (!featureValues.containsKey(featurename)) {
                featureValues.put(featurename, 0);
            }
        }
    }

    /**
     * #retweet #like #hashmap url
     */
    private void tweetFeatures() {
        if (status != null) {

        }
    }

    /**
     * #retweet #following #follower
     */
    private void userFeatures() {
        if (status != null) {

        }
    }

}
