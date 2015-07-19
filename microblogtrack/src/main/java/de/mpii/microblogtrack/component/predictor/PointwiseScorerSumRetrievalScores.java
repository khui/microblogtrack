package de.mpii.microblogtrack.component.predictor;

import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import static de.mpii.microblogtrack.utility.QueryTweetPair.concatModelQuerytypeFeature;
import gnu.trove.TCollections;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import java.util.ArrayList;
import java.util.List;
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

    private final String[] retrivemodelName;
    // we consider the top-6 highest retrieval score, using the sum of them as the final score
    // since in total we have three fields, thus these top-6 means at least two models are telling
    // their blief of this doc
    private final int topScore2Consider = 6;

    public PointwiseScorerSumRetrievalScores() {
        List<String> fnames = new ArrayList<>();
        for (String querytype : Configuration.QUERY_TYPES) {
            for (String model : Configuration.FEATURES_RETRIVEMODELS) {
                String featurename = concatModelQuerytypeFeature(model, querytype);
                fnames.add(featurename);
            }
        }
        fnames.add(Configuration.TWEET_URL_TITLE);
        retrivemodelName = fnames.toArray(new String[0]);
    }

    @Override
    public double predictor(QueryTweetPair qtr) {
        TObjectDoubleMap<String> featureValues = qtr.getFeatures();
        TDoubleList scores = new TDoubleArrayList();
        // update the min/max for each features
        for (String model : retrivemodelName) {
            if (featureValues.containsKey(model)) {
                double value = featureValues.get(model);
                if (value == Double.NaN) {
                    logger.error(model + " returns feature value " + value);
                    continue;
                }
                synchronized (feature_max) {
                    if (value > feature_max.get(model)) {
                        feature_max.put(model, value);
                        scores.add(1);
                    } else if (value < feature_min.get(model)) {
                        feature_min.put(model, value);
                        scores.add(0);
                    } else {
                        if (feature_min.get(model) != feature_max.get(model)) {
                            value = (value - feature_min.get(model)) / (feature_max.get(model) - feature_min.get(model));
                        } else {
                            value = 0;
                        }
                        scores.add(value);
                    }
                }
            }
        }
        double finalScore = 0;
        if (scores.size() > 1) {
            try {
                scores.sort();
                scores.reverse();
                // we remove the highest score
                for (int i = 1; i < scores.size(); i++) {
                    if (i > topScore2Consider) {
                        break;
                    }
                    finalScore += scores.get(i);
                }
            } catch (Exception ex) {
                logger.error("", ex);
            }
        }
        qtr.setPredictScore(Configuration.PRED_ABSOLUTESCORE, finalScore);
        return finalScore;
    }

}
