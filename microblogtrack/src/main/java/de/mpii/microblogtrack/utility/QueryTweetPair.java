package de.mpii.microblogtrack.utility;

import de.bwaldvogel.liblinear.Feature;
import de.bwaldvogel.liblinear.FeatureNode;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import libsvm.svm_node;
import org.apache.log4j.Logger;
import twitter4j.HashtagEntity;
import twitter4j.MediaEntity;
import twitter4j.Status;
import twitter4j.URLEntity;
import twitter4j.User;
import twitter4j.UserMentionEntity;

/**
 *
 * @author khui
 */
public class QueryTweetPair {

    static Logger logger = Logger.getLogger(QueryTweetPair.class.getName());

    protected final TObjectDoubleMap<String> featureValues = new TObjectDoubleHashMap<>();

    protected final TObjectDoubleMap<String> predictorResults = new TObjectDoubleHashMap<>();

    protected static String[] featureNames = generateFeatureNameString();

    protected Map<String, String> contentString = new HashMap<>();

    public final long tweetid;

    public final String queryid;

    protected svm_node[] vectorLibsvm = null;

    protected Feature[] vectorLiblinear = null;

    protected QueryTweetPair(long tweetid, String queryid) {
        this.tweetid = tweetid;
        this.queryid = queryid;
    }

    public QueryTweetPair(long tweetid, String queryid, Status status, String urltitle) {
        this.tweetid = tweetid;
        this.queryid = queryid;
        updateUserTweetFeatures(status, urltitle);
    }

    public QueryTweetPair(long tweetid, String queryid, Status status) {
        this(tweetid, queryid, status, null);
    }

    public QueryTweetPair(QueryTweetPair qtp) {
        this.tweetid = qtp.tweetid;
        this.queryid = qtp.queryid;
        this.featureValues.clear();
        this.predictorResults.clear();
        this.featureValues.putAll(qtp.getFeatures());
        this.predictorResults.putAll(qtp.getPredictRes());
        this.contentString = new HashMap(qtp.contentString);
        this.vectorLibsvm = qtp.vectorLibsvm;
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

    public static String concatModelQuerytypeFeature(String model, String querytype) {
        return model + "_" + querytype;
    }

    public static String[] generateFeatureNameString() {
        List<String> featurenames = new ArrayList<>();
        for (String querytype : Configuration.QUERY_TYPES) {
            for (String model : Configuration.FEATURES_RETRIVEMODELS) {
                String featurename = concatModelQuerytypeFeature(model, querytype);
                featurenames.add(featurename);
            }
        }
        featurenames.addAll(Arrays.asList(Configuration.FEATURES_TWEETQUALITY));
        featurenames.addAll(Arrays.asList(Configuration.FEATURES_USERAUTHORITY));
        featurenames.add(Configuration.TWEET_URL_TITLE);
        Collections.sort(featurenames);
        featureNames = featurenames.toArray(new String[0]);
        logger.info("Generated feature name array for " + featureNames.length + " features:");
        StringBuilder sb;
        for (int i = 0; i < featureNames.length; i++) {
            sb = new StringBuilder();
            System.out.println(sb.append(i+1).append(":").append(featureNames[i]));
        }

        return featureNames;
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

    public Map<String, String> getContentStr() {
        return contentString;
    }

    public void setURLTitle(String str) {
        contentString.put(Configuration.TWEET_URL_TITLE, str);
    }

    public void setPredictScore(String predictresultname, double predictscore) {
        this.predictorResults.put(predictresultname, predictscore);
    }

    public double getAbsScore() {
        double score = -1;
        if (predictorResults.containsKey(Configuration.PRED_ABSOLUTESCORE)) {
            score = predictorResults.get(Configuration.PRED_ABSOLUTESCORE);
        }
        return score;
    }

    public double getRelScore() {
        double score = -1;
        if (predictorResults.containsKey(Configuration.PRED_RELATIVESCORE)) {
            score = predictorResults.get(Configuration.PRED_RELATIVESCORE);
        }
        return score;
    }

    /**
     * convert the feature map to the svm_node, the data format used in libsvm
     * note that the libsvm supposes the feature index starts from 1, with min
     * max scale
     *
     * @param featureMinMax
     * @return
     */
    public svm_node[] vectorizeLibsvmMinMax(Map<String, double[]> featureMinMax) {
        TObjectDoubleMap<String> scaledFeatureValues = rescaleFeaturesMinMax(featureMinMax);
        List<svm_node> nodes = new ArrayList<>();
        for (int i = 1; i <= featureNames.length; i++) {
            if (featureValues.containsKey(featureNames[i - 1])) {
                double fvalue = scaledFeatureValues.get(featureNames[i - 1]);
                if (fvalue != 0) {
                    nodes.add(new svm_node());
                    nodes.get(nodes.size() - 1).index = i;
                    nodes.get(nodes.size() - 1).value = fvalue;
                }
            }
        }
        vectorLibsvm = nodes.toArray(new svm_node[0]);
        return vectorLibsvm;
    }

    /**
     * convert the feature map to the svm_node, the data format used in libsvm
     * note that the libsvm supposes the feature index starts from 1, with mean
     * std scale
     *
     * @param featureMeanStd
     * @return
     */
    public svm_node[] vectorizeLibsvmMeanStd(Map<String, double[]> featureMeanStd) {
        TObjectDoubleMap<String> scaledFeatureValues = rescaleFeaturesMeanStd(featureMeanStd);
        List<svm_node> nodes = new ArrayList<>();
        for (int i = 1; i <= featureNames.length; i++) {
            if (scaledFeatureValues.containsKey(featureNames[i - 1])) {
                double fvalue = scaledFeatureValues.get(featureNames[i - 1]);
                if (fvalue != 0) {
                    nodes.add(new svm_node());
                    nodes.get(nodes.size() - 1).index = i;
                    nodes.get(nodes.size() - 1).value = fvalue;
                }
            }
        }
        vectorLibsvm = nodes.toArray(new svm_node[0]);
        return vectorLibsvm;
    }

    public static String[] getFeatureNames() {
        return featureNames;
    }

    public Feature[] vectorizeLiblinearMeanStd(Map<String, double[]> featureMeanStd) {
        TObjectDoubleMap<String> scaledFeatureValues = rescaleFeaturesMeanStd(featureMeanStd);
        List<Feature> nodes = new ArrayList<>();
        for (int i = 1; i <= featureNames.length; i++) {
            if (scaledFeatureValues.containsKey(featureNames[i])) {
                double fvalue = scaledFeatureValues.get(featureNames[i - 1]);
                if (fvalue != 0) {
                    nodes.add(new FeatureNode(i, fvalue));
                }
            }
        }
        vectorLiblinear = nodes.toArray(new Feature[0]);
        return vectorLiblinear;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(queryid).append(":").append(tweetid).append(" ");
//        for (String featurename : Configuration.FEATURES_RETRIVEMODELS) {
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
     * @return
     */
    private TObjectDoubleMap<String> rescaleFeaturesMeanStd(Map<String, double[]> featureMeanStd) {
        double std, mean, r_value, n_value;
        TObjectDoubleMap<String> scaledFeatureValues = new TObjectDoubleHashMap<>();
        String[] features = featureValues.keySet().toArray(new String[0]);
        for (String feature : features) {
            r_value = featureValues.get(feature);
            if (Configuration.FEATURES_NO_SCALE.contains(feature)) {
                scaledFeatureValues.put(feature, r_value);
                continue;
            }
            if (featureMeanStd.containsKey(feature)) {
                mean = featureMeanStd.get(feature)[0];
                std = featureMeanStd.get(feature)[1];
                // we need to confirm the std is larger than zero
                if (std > 0) {
                    n_value = (r_value - mean) / std;
                    scaledFeatureValues.put(feature, n_value);
                } else {
                    logger.error("std is zero for " + feature);
                }
            }
        }
        return scaledFeatureValues;
    }

    /**
     * min/max scaling method, rescale each feature to [0, 1], afterward the
     * vector representations are rebuilt. The min/max value for each feature
     * comes from off-line computation
     *
     * @param featureMinMax
     */
    private TObjectDoubleMap<String> rescaleFeaturesMinMax(Map<String, double[]> featureMinMax) {
        double max, min, difference, r_value, n_value;
        TObjectDoubleMap<String> scaledFeatureValues = new TObjectDoubleHashMap<>();

        String[] features = featureValues.keySet().toArray(new String[0]);
        for (String feature : features) {
            r_value = featureValues.get(feature);
            if (Configuration.FEATURES_NO_SCALE.contains(feature)) {
                scaledFeatureValues.put(feature, r_value);
                continue;
            }
            if (featureMinMax.containsKey(feature)) {

                min = featureMinMax.get(feature)[0];
                max = featureMinMax.get(feature)[1];
                if (min < max) {
                    difference = max - min;
                    n_value = (r_value - min) / difference;
                    scaledFeatureValues.put(feature, n_value);
                } else {
                    logger.error("min lte max for " + feature + " : " + min + " " + max);
                }
            }
        }
        return scaledFeatureValues;
    }

    protected final void updateUserTweetFeatures(Status status, String urltitle) {
        tweetFeatures(status);
        userFeatures(status);
        this.contentString.put(Configuration.TWEET_CONTENT, status.getText());
        if (urltitle != null) {
            this.contentString.put(Configuration.TWEET_URL_TITLE, urltitle);
        }
    }

    /**
     * #retweet #like #hashmap url
     */
    private void tweetFeatures(Status status) {
        if (status != null) {
            double featureV = -1;
            for (String feature : Configuration.FEATURES_TWEETQUALITY) {
                switch (feature) {
                    case Configuration.FEATURE_T_FAVORITENUM:
                        featureV = status.getFavoriteCount();
                        break;
                    case Configuration.FEATURE_T_HASHTAGNUM:
                        HashtagEntity[] hashtagentity = status.getHashtagEntities();
                        if (hashtagentity == null) {
                            break;
                        }
                        featureV = hashtagentity.length;
                        break;
                    case Configuration.FEATURE_T_RETWEETNUM:
                        featureV = status.getRetweetCount();
                        break;
                    case Configuration.FEATURE_T_URLNUM:
                        URLEntity[] urlentity = status.getURLEntities();
                        if (urlentity == null) {
                            break;
                        }
                        featureV = urlentity.length;
                        break;
                    case Configuration.FEATURE_T_USERMENTIONNUM:
                        UserMentionEntity[] mentionentity = status.getUserMentionEntities();
                        if (mentionentity == null) {
                            break;
                        }
                        featureV = mentionentity.length;
                        break;
                    case Configuration.FEATURE_T_LENGTH:
                        String content = status.getText();
                        if (content == null) {
                            break;
                        }
                        featureV = content.length();
                        break;
                }
                if (featureV > 0) {
                    featureValues.put(feature, featureV);
                }
            }
        }
    }

    /**
     * #retweet #following #follower
     */
    private void userFeatures(Status status) {
        if (status != null) {
            User user = status.getUser();
            double featureV = -1;
            for (String feature : Configuration.FEATURES_USERAUTHORITY) {
                if (user == null) {
                    logger.error("User from status is null");
                    break;
                }
                switch (feature) {
                    case Configuration.FEATURE_U_DESC_LEN:
                        String description = user.getDescription();
                        if (description == null) {
                            break;
                        }
                        featureV = description.length();
                        break;
                    case Configuration.FEATURE_U_DESC_URLNUM:
                        URLEntity[] uentity = user.getDescriptionURLEntities();
                        if (uentity == null) {
                            break;
                        }
                        featureV = uentity.length;
                        break;
                    case Configuration.FEATURE_U_FAVORITENUM:
                        featureV = user.getFavouritesCount();
                        break;
                    case Configuration.FEATURE_U_FOLLOWERNUM:
                        featureV = user.getFollowersCount();
                        break;
                    case Configuration.FEATURE_U_FRIENDNUM:
                        featureV = user.getFriendsCount();
                        break;
                    case Configuration.FEATURE_U_ISCELEBRITY:
                        featureV = (user.isVerified() ? 1 : 0);
                        break;
                    case Configuration.FEATURE_U_ISDEFAULT_ICON:
                        featureV = (user.isDefaultProfileImage() ? 0 : 1);
                        break;
                    case Configuration.FEATURE_U_LISTNUM:
                        featureV = (user.getListedCount() > 0 ? user.getListedCount() : 0);
                        break;
                    case Configuration.FEATURE_U_STATUSNUM:
                        featureV = user.getStatusesCount();
                        break;
                }
                if (featureV > 0) {
                    featureValues.put(feature, featureV);
                }
            }
        }
    }

}
