package de.mpii.microblogtrack.utility;

import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.math3.random.EmpiricalDistribution;
import org.apache.log4j.Logger;
import org.apache.mahout.clustering.streaming.cluster.BallKMeans;
import org.apache.mahout.clustering.streaming.cluster.StreamingKMeans;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Centroid;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.neighborhood.UpdatableSearcher;
import org.apache.mahout.math.neighborhood.ProjectionSearch;

/**
 * main facility for ranking and pointwise prediction. for each query, we
 * construct one ResultTweetsTracker instance, keeping track of the relevant
 * tweets for this query, meanwhile keeping track of the centroid by streaming
 * k-means..
 *
 * @author khui
 */
public class ResultTrackerKMean implements ResultTweetsTracker {

    static Logger logger = Logger.getLogger(ResultTweetsTracker.class);

    public final String queryid;

    //private final int numberOfTweetsToKeep = MYConstants.TOP_N_FROM_LUCENE * MYConstants.RECORD_MINIUTES;
    // track the score of the tweets for the query to generate relative score for each upcoming tweets
    // we only track the scores for the latest 24 hours
    private final int numberOfScoreToTrack = MYConstants.TOP_N_FROM_LUCENE * 3600;

    private EmpiricalDistribution edistPredictScore = null;
    // summarized pointwise prediction results for the computation of empirical dist
    private final TIntDoubleMap pointwiseScore = new TIntDoubleHashMap(numberOfScoreToTrack);

    private final String[] scorenames = new String[]{MYConstants.PRED_ABSOLUTESCORE};

    private final UpdatableSearcher streamKMCentroids;

    private final StreamingKMeans clusterer;

    private final String distanceMeasure = MYConstants.DISTANT_MEASURE_CLUSTER;

    private final int numProjections = 20;

    private final int searchSize = 10;

    private int tweetcount = 0;

    public ResultTrackerKMean(String queryid) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        this.queryid = queryid;
        //this.tidFeatures = TCollections.synchronizedMap(new TIntObjectHashMap<>(numberOfTweetsToKeep));
        // initialize an empty streamKMCentroids set
        this.streamKMCentroids = new ProjectionSearch((DistanceMeasure) Class.forName(distanceMeasure).newInstance(), numProjections, searchSize);
        this.clusterer = new StreamingKMeans(streamKMCentroids, MYConstants.STREAMKMEAN_CLUSTERNUM);
    }

    /**
     * add a tweet to this query profile: add features, add scores from the
     * pointwise prediction procedure. note that both the features and the
     * prediction scores can be multi dimensions. The example for the latter is,
     * the outcome of the svm prediction, normally including confidence, the
     * distance to the hyperplane etc..
     *
     * @param qtp
     */
    @Override
    public void addTweet(QueryTweetPair qtp) {
        tweetcount++;

        long tweetid = qtp.tweetid;
        Vector v = qtp.vectorize();
        double score = summarizePointwisePrediction(tweetid, qtp.getPredictRes());
        // the unique predicting score for one tweet, and the corresponding cumulative prob is used as vector weight
        double prob = getCumulativeProb(score);

        // use the tweet count as the centroid key
        synchronized (clusterer) {
            clusterer.cluster(new Centroid(tweetcount, v.clone(), prob));
        }
    }

    @Override
    public double avgDistCentroids() {
        return 0;
    }

    @Override
    public double relativeScore(double absoluteScore) {
        return 0;
    }

    /**
     * core function for decision making: 1) get updated centroidnum centroids;
     * 2) search among the latest tweets, to get the closest searchSize tweets
     * and record their score cumulative probability (the higher the better),
     * distance information in CandidateTweet object; 3) the final decision is
     * based on both the relative probability and the distance to the centroid.
     * Note that, the centroids are changed with time, thus no absolute
     * centroids are available
     *
     * @param centroidnum
     * @return
     */
    private Iterable<Vector> getCentroids(int centroidnum) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        int maxNumIterations = MYConstants.MAX_ITERATE_BALLKMEAN;
        UpdatableSearcher emptySercher = new ProjectionSearch((DistanceMeasure) Class.forName(distanceMeasure).newInstance(), numProjections, searchSize);
        BallKMeans exactCentroids = new BallKMeans(emptySercher, centroidnum, maxNumIterations);
        List<Centroid> centroidList = new ArrayList<>();
        synchronized (clusterer) {
            clusterer.reindexCentroids();
            Iterator<Centroid> it = clusterer.iterator();
            while (it.hasNext()) {
                centroidList.add(it.next().clone());
            }
        }
        return exactCentroids.cluster(centroidList);
    }

    private double getCumulativeProb(double score) {
        double prob = 0.5;
        if (edistPredictScore != null) {
            prob = edistPredictScore.cumulativeProbability(score);
        }
        return prob;
    }

    private double summarizePointwisePrediction(long tweetid, TObjectDoubleMap<String> predictScores) {
        double[] scores = new double[scorenames.length];
        double presentativescore = 0;
        for (int i = 0; i < scorenames.length; i++) {
            if (predictScores.containsKey(scorenames[i])) {
                scores[i] = predictScores.get(scorenames[i]);
                if (scorenames[i].equals(MYConstants.PRED_ABSOLUTESCORE)) {
                    presentativescore = scores[i];
                    pointwiseScore.put(tweetcount % numberOfScoreToTrack, presentativescore);
                    // reload the estimate of probility per 10000 entries
                    if (pointwiseScore.size() % 10000 == 0 && edistPredictScore != null) {
                        this.edistPredictScore = new EmpiricalDistribution();
                        edistPredictScore.load(pointwiseScore.values(new double[]{}));
                    }
                }
            }
        }
        return presentativescore;
    }

}
