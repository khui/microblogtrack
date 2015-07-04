package de.mpii.microblogtrack.utility;

import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import libsvm.svm_node;
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

    private Vector vectorMahout = null;

    private svm_node[] vectorLibsvm = null;

    private Map<String, double[]> featureMeanStd;

    public QueryTweetPair(long tweetid, String queryid, Status status) {
        this.tweetid = tweetid;
        this.queryid = queryid;
        this.status = status;
        this.featureMeanStd = new HashMap<>();
        updateFeatures();
    }

    public QueryTweetPair(long tweetid, String queryid, Status status, Map<String, double[]> featureMeanStd) {
        this.tweetid = tweetid;
        this.queryid = queryid;
        this.status = status;
        this.featureMeanStd = featureMeanStd;
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
        this.vectorMahout = qtp.vectorizeMahout();
        this.featureMeanStd = qtp.featureMeanStd;
    }

    public void updateMeanStdScaler(Map<String, double[]> featureMeanStd) {
        this.featureMeanStd = featureMeanStd;
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
        if (vectorMahout != null) {
            return vectorMahout;
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
        vectorMahout = new DenseVector(fvalues);
        return vectorMahout;
    }

    /**
     * convert the feature map to the svm_node, the data format used in libsvm
     * note that the libsvm supposes the feature index starts from 1
     *
     * @return
     */
    public svm_node[] vectorizeLibsvm() {
        if (vectorLibsvm != null) {
            return vectorLibsvm;
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

        List<svm_node> nodes = new ArrayList<>();
        for (int i = 1; i <= featureNames.length; i++) {
            double fvalue = featureValues.get(featureNames[i]);
            if (fvalue > 0) {
                nodes.add(new svm_node());
                nodes.get(nodes.size() - 1).index = i;
                nodes.get(nodes.size() - 1).value = fvalue;

            }
        }
        vectorLibsvm = nodes.toArray(new svm_node[0]);
        return vectorLibsvm;
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
     * we use mean/std scaling method, rescale each feature to Norm(0, 1),
     * afterward the vector representations are rebuilt. The mean/std value for
     * each feature can either come from off-line computation
     *
     * @param featureMeanStd
     */
    public void rescaleFeatures() {
        if (featureMeanStd.isEmpty()) {
            return;
        }
        double std, mean, r_value, n_value;
        String[] features = featureValues.keySet().toArray(new String[0]);
        for (String feature : features) {
            if (featureMeanStd.containsKey(feature)) {
                r_value = featureValues.get(feature);
                mean = featureMeanStd.get(feature)[0];
                std = featureMeanStd.get(feature)[1];
                // we need to confirm the std is larger than zero
                if (std > 0) {
                    n_value = (r_value - mean) / std;
                    featureValues.put(feature, n_value);
                } else {
                    logger.error("std is zero for " + feature);
                }
            }
        }
        if (vectorMahout != null) {
            vectorMahout = null;
            vectorizeMahout();
        }
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
