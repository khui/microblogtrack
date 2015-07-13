package de.mpii.microblogtrack.component.predictor;

import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import gnu.trove.TCollections;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import org.apache.log4j.Logger;

/**
 * for pointwise prediction, more complicated predictor should extends this
 * class and override the predictor
 *
 * @author khui
 */
public class PointwiseScorerArregate implements PointwiseScorer {

    static Logger logger = Logger.getLogger(PointwiseScorerArregate.class);

    private final TObjectDoubleMap<String> feature_min = TCollections.synchronizedMap(new TObjectDoubleHashMap<>());

    private final TObjectDoubleMap<String> feature_max = TCollections.synchronizedMap(new TObjectDoubleHashMap<>());

    private final String[] models;

    public PointwiseScorerArregate() {
        for (String querytype : Configuration.QUERY_TYPES) {
            for (String model : Configuration.FEATURES_RETRIVEMODELS) {
                String featurename = QueryTweetPair.concatModelQuerytypeFeature(model, querytype);
                feature_min.put(featurename, Double.MAX_VALUE);
                feature_max.put(featurename, -Double.MAX_VALUE);
            }
        }
        models = feature_max.keys(new String[0]);
    }

    @Override
    public double predictor(QueryTweetPair qtr) {
        TObjectDoubleMap<String> featureValues = qtr.getFeatures();
        double sum = 0;
        // update the min/max for each features
        for (String model : models) {
            if (featureValues.containsKey(model)) {
                double value = featureValues.get(model);
                synchronized (feature_max) {
                    if (value > feature_max.get(model)) {
                        feature_max.adjustValue(model, value);
                        sum += 1;
                    } else if (value < feature_min.get(model)) {
                        feature_min.adjustValue(model, value);
                        sum += 0;
                    } else {
                        value = (value - feature_min.get(model)) / (feature_max.get(model) - feature_min.get(model));
                        sum += value;
                    }
                }
            }
        }
        sum /= models.length;
        qtr.setPredictScore(Configuration.PRED_ABSOLUTESCORE, sum);
        return sum;
    }

}
