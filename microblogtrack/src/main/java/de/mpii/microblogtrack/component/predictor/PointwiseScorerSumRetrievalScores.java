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
public class PointwiseScorerSumRetrievalScores implements PointwiseScorer {
    
    static Logger logger = Logger.getLogger(PointwiseScorerSumRetrievalScores.class);
    
    private final TObjectDoubleMap<String> feature_min = TCollections.synchronizedMap(new TObjectDoubleHashMap<>());
    
    private final TObjectDoubleMap<String> feature_max = TCollections.synchronizedMap(new TObjectDoubleHashMap<>());
    
    private final String[] featurenames;
    
    public PointwiseScorerSumRetrievalScores() {
        featurenames = QueryTweetPair.getFeatureNames();
        if (featurenames == null) {
            logger.error("featurenames array is " + featurenames);
        }
    }
    
    @Override
    public double predictor(QueryTweetPair qtr) {
        TObjectDoubleMap<String> featureValues = qtr.getFeatures();
        double sum = 0;
        // update the min/max for each features
        for (String model : featurenames) {
            if (featureValues.containsKey(model)) {
                double value = featureValues.get(model);
                if (value == Double.NaN) {
                    logger.error(model + " returns feature value " + value);
                    continue;
                }
                synchronized (feature_max) {
                    if (value > feature_max.get(model)) {
                        feature_max.put(model, value);
                        sum += 1;
                    } else if (value < feature_min.get(model)) {
                        feature_min.put(model, value);
                        sum += 0;
                    } else {
                        if (feature_min.get(model) != feature_max.get(model)) {
                            value = (value - feature_min.get(model)) / (feature_max.get(model) - feature_min.get(model));
                        } else {
                            value = 0;
                        }
                        sum += value;
                    }
                }
            }
        }
        sum /= featurenames.length;
        
        qtr.setPredictScore(Configuration.PRED_ABSOLUTESCORE, sum);
        return sum;
    }
    
}
