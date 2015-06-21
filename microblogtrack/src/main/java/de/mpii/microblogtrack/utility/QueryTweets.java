package de.mpii.microblogtrack.utility;

import gnu.trove.list.TDoubleList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import org.apache.commons.math3.random.EmpiricalDistribution;
import org.apache.log4j.Logger;

/**
 *
 * @author khui
 */
public class QueryTweets {

    static Logger logger = Logger.getLogger(QueryTweets.class);

    public final String queryid;

    private final TLongObjectMap<double[]> tidFeatures;

    private final TLongObjectMap<double[]> tidPointwiseScores;

    private EmpiricalDistribution edistPredictScore = null;
    // summarized pointwise prediction results for the computation of empirical dist
    private final TDoubleList pointwiseScore;

    private final String[] featurenames = new String[]{};

    private final String[] scorenames = new String[]{MYConstants.PREDICTSCORE};

    public QueryTweets(String queryid) {
        this.queryid = queryid;
        this.tidFeatures = new TLongObjectHashMap<>();
        this.tidPointwiseScores = new TLongObjectHashMap<>();
        this.pointwiseScore = new TDoubleArrayList();
    }

    /**
     * add a tweet to this query profile: add features, add scores from the
     * pointwise prediction procedure. note that both the features and the
     * prediction scores can be multi dimensions. The example for the latter is,
     * the outcome of the svm prediction, normally including confidence, the
     * distance to the hyperplane etc..
     *
     * @param tweetid
     * @param featureValues
     * @param predictScores
     */
    public void addTweet(long tweetid, TObjectDoubleMap<String> featureValues, TObjectDoubleMap<String> predictScores) {
        double[] features = new double[featurenames.length];

        for (int i = 0; i < featurenames.length; i++) {
            if (featureValues.containsKey(featurenames[i])) {
                features[i] = featureValues.get(featurenames[i]);
            } else {
                logger.error(featurenames[i] + " is not in the feature-value pairs.");
                features[i] = 0;
            }
        }
        tidFeatures.put(tweetid, features);
        summarizePointwisePrediction(tweetid, predictScores);
    }

    public double getCumulativeProb(double score) {
        return edistPredictScore.cumulativeProbability(score);
    }

    private void summarizePointwisePrediction(long tweetid, TObjectDoubleMap<String> predictScores) {
        double[] scores = new double[featurenames.length];
        for (int i = 0; i < scorenames.length; i++) {
            if (predictScores.containsKey(scorenames[i])) {
                scores[i] = predictScores.get(scorenames[i]);
                if (scorenames[i].equals(MYConstants.PREDICTSCORE)) {
                    pointwiseScore.add(scores[i]);
                    // reload the estimate of probility per 10000 entries
                    if (pointwiseScore.size() % 10000 == 0 && edistPredictScore != null) {
                        this.edistPredictScore = new EmpiricalDistribution();
                        edistPredictScore.load(pointwiseScore.toArray());
                    }
                }
            } else {
                logger.error(scorenames[i] + " is not in the feature-value pairs.");
                scores[i] = 0;
            }
        }
        tidPointwiseScores.put(tweetid, scores);

    }
}
