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

    private final TObjectDoubleMap<String> featureValues = new TObjectDoubleHashMap<>();

    private final TObjectDoubleMap<String> predictorResults = new TObjectDoubleHashMap<>();

    private static String[] featureNames = null;

    private Map<String, String> contentString = new HashMap<>();

    public final long tweetid;

    public final String queryid;

    private svm_node[] vectorLibsvm = null;

    public QueryTweetPair(long tweetid, String queryid, Status status, String urltitle) {
        this.tweetid = tweetid;
        this.queryid = queryid;
        updateFeatures(status, urltitle);
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
            double fvalue = featureValues.get(featureNames[i - 1]);
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
     */
    public void rescaleFeatures(Map<String, double[]> featureMeanStd) {
        double std, mean, r_value, n_value;
        String[] features = featureValues.keySet().toArray(new String[0]);
        for (String feature : features) {
            if (Configuration.FEATURES_NO_SCALE.contains(feature)) {
                continue;
            }
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
        if (vectorLibsvm != null) {
            vectorLibsvm = null;
            vectorizeLibsvm();
        }
    }

    protected final void updateFeatures(Status status, String urltitle) {
        semanticFeatures();
        tweetFeatures(status);
        userFeatures(status);
        this.contentString.put(Configuration.TWEET_CONTENT, status.getText());
        if (urltitle != null) {
            this.contentString.put(Configuration.TWEET_URL_TITLE, urltitle);
        }
    }

    /**
     * semantic matching features
     */
    private void semanticFeatures() {
        for (String querytype : Configuration.QUERY_TYPES) {
            for (String model : Configuration.FEATURES_RETRIVEMODELS) {
                String featurename = concatModelQuerytypeFeature(model, querytype);
                if (!featureValues.containsKey(featurename)) {
                    featureValues.put(featurename, 0);
                }
            }
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
                    case Configuration.FEATURE_T_MEDIANUM:
                        MediaEntity[] mediaentity = status.getMediaEntities();
                        if (mediaentity == null) {
                            break;
                        }
                        featureV = mediaentity.length;
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
                }
                if (featureV > 0) {
                    featureValues.put(feature, featureV);
                } else {
                    featureValues.put(feature, 0);
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
                } else {
                    featureValues.put(feature, 0);
                }
            }
        }
    }

}
