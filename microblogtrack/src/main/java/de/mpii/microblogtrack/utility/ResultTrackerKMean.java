package de.mpii.microblogtrack.utility;

import gnu.trove.TCollections;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.TDoubleIntMap;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TDoubleIntHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

    private final Map<String, double[]> featureMeanStd;

    // record the occrrence for each predict score, generating the approximating cumulative distribution
    private final TDoubleIntMap predictScoreTracker = TCollections.synchronizedMap(new TDoubleIntHashMap());

    // centroid number, default value is 10, governing how many classes we may have after clustering
    private int centroidnum = 10;
    // average distance among centroids, as reference in computing distance between two tweets
    private double avgCentroidDistance = 0.01;
    // identification for each tweet
    private int tweetcount = 0;

    /**
     * the fields below are for clustering algorithm
     */
    private final UpdatableSearcher streamKMCentroids;

    private final StreamingKMeans clusterer;

    private final DistanceMeasure distanceMeasure;

    private final int numProjections = 20;

    private final int searchSize = 10;

    private boolean isStarted = false;

    public ResultTrackerKMean(String queryid) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        this.queryid = queryid;
        this.distanceMeasure = (DistanceMeasure) Class.forName(Configuration.TRACKER_DISTANT_MEASURE).newInstance();
        // initialize an empty streamKMCentroids set
        this.streamKMCentroids = new ProjectionSearch(distanceMeasure, numProjections, searchSize);
        this.clusterer = new StreamingKMeans(streamKMCentroids, Configuration.TRACKER_SKMEAN_CLUSTERNUM_UPBOUND);
        this.featureMeanStd = new ConcurrentHashMap<>();
    }

    public ResultTrackerKMean(String queryid, Map<String, double[]> featureMeanStd) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        this.queryid = queryid;
        this.distanceMeasure = (DistanceMeasure) Class.forName(Configuration.TRACKER_DISTANT_MEASURE).newInstance();
        // initialize an empty streamKMCentroids set
        this.streamKMCentroids = new ProjectionSearch(distanceMeasure, numProjections, searchSize);
        this.clusterer = new StreamingKMeans(streamKMCentroids, Configuration.TRACKER_SKMEAN_CLUSTERNUM_UPBOUND);
        this.featureMeanStd = featureMeanStd;
    }

    /**
     * add a tweet to this query profile: add features, add scores from the
     * pointwise prediction procedure. note that both the features and the
     * prediction scores can be multi dimensions. The example for the latter is,
     * the outcome of the svm prediction, normally including confidence, the
     * distance to the hyperplane etc..
     *
     * @param qtps
     */
    @Override
    public void addTweets(Collection<QueryTweetPair> qtps) {
        Vector v;
        double absoluteScore, relativeScore;
        List<Centroid> datapoints2add = new ArrayList<>();
        for (QueryTweetPair qtp : qtps) {
            tweetcount++;
            v = qtp.vectorizeMahout();
            // the unique predicting score for one tweet, and the corresponding cumulative prob is used as vector weight
            absoluteScore = trackPredictScore(qtp.getPredictRes());
            // add the tweet to the clustering, using the tweet count as the centroid key
            relativeScore = getCumulativeProb(absoluteScore);

            qtp.setPredictScore(Configuration.PRED_RELATIVESCORE, relativeScore);
            datapoints2add.add(new Centroid(tweetcount, v.clone(), relativeScore));
            // update the average distance among centroids every x miniutes
            if (tweetcount % (Configuration.LUCENE_TOP_N_SEARCH * Configuration.TRACKER_AVGDIST_UPDATE_MINUTES) == 0) {
                try {
                    updateAvgCentroidDist(this.centroidnum);
                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
                    logger.error(ex.getMessage());
                }
            }
        }
        updateCentroid(datapoints2add);
    }

//    @Override
//    public void updateFeatureMinMax(QueryTweetPair qtp) {
//        TObjectDoubleMap<String> featureValues = qtp.getFeatures();
//        double value, min, max;
//        for (String feature : featureValues.keySet()) {
//            value = featureValues.get(feature);
//            if (!featureMinMax.containsKey(feature)) {
//                featureMinMax.put(feature, new double[2]);
//                featureMinMax.get(feature)[0] = Double.MAX_VALUE;
//                featureMinMax.get(feature)[1] = Double.MIN_VALUE;
//            }
//            min = featureMinMax.get(feature)[0];
//            max = featureMinMax.get(feature)[1];
//            if (value < min) {
//                featureMinMax.get(feature)[0] = value;
//            } else if (value > max) {
//                featureMinMax.get(feature)[1] = value;
//            }
//
//        }
//    }
    @Override
    public synchronized boolean isStarted() {
        return isStarted;
    }

    @Override
    public synchronized void informStart2Record() {
        isStarted = true;
    }

    @Override
    public Map<String, double[]> getMeanStdScaler() {
        return featureMeanStd;
    }
    
    @Override
    public double avgDistCentroids() {
        return avgCentroidDistance;
    }

    @Override
    public double relativeScore(double absoluteScore) {
        return getCumulativeProb(absoluteScore);
    }

    @Override
    public void setCentroidNum(int centroidnum) {
        this.centroidnum = centroidnum;
    }

    private synchronized void updateCentroid(Iterable<Centroid> centroid2add) {
        clusterer.cluster(centroid2add);
    }

    private void updateAvgCentroidDist(int centroidnum) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        double distance = 0;
        double count = 1;
        List<? extends Vector> centroidsList = getCentroids(centroidnum);
        for (int i = 0; i < centroidsList.size(); i++) {
            for (int j = i + 1; j < centroidsList.size(); j++) {
                Vector c_x = centroidsList.get(i);
                Vector c_y = centroidsList.get(j);
                distance += distanceMeasure.distance(c_x, c_y);
                count++;
            }
        }
        avgCentroidDistance = (distance / count > 0 ? distance / count : 0.01);
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
    private List<? extends Vector> getCentroids(int centroidnum) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        int maxNumIterations = Configuration.TRACKER_BALLKMEAN_MAX_ITERATE;
        UpdatableSearcher emptySercher = new ProjectionSearch(distanceMeasure, numProjections, searchSize);
        List<Centroid> centroidList = new ArrayList<>();
        synchronized (clusterer) {
            clusterer.reindexCentroids();
            Iterator<Centroid> it = clusterer.iterator();
            while (it.hasNext()) {
                centroidList.add(it.next().clone());
            }
        }
        // if we have less than centroidnum centroids
        if (centroidList.size() <= centroidnum) {
            return centroidList;
        }
        BallKMeans exactCentroids = new BallKMeans(emptySercher, centroidnum, maxNumIterations);
        UpdatableSearcher resultSearch = exactCentroids.cluster(centroidList);
        Iterator<Vector> clusteredCentroids = resultSearch.iterator();
        List<Vector> vectorList = new ArrayList<>();
        while (clusteredCentroids.hasNext()) {
            vectorList.add(clusteredCentroids.next().clone());
        }
        return vectorList;
    }

    /**
     * return the cumulative probability for the top
 Configuration.TRACKER_CUMULATIVE_TOPPERC tweets
     *
     * @param score
     * @return
     */
    public double getCumulativeProb(double score) {
        // default value
        double prob = 1 - Configuration.TRACKER_CUMULATIVE_TOPPERC;
        int cumulativeCount = 0;

        int currentTweetCount = this.tweetcount;
        // when we dont have enough tweets, we return the default directly as relative score
        if (currentTweetCount < Configuration.TRACKER_CUMULATIVE_GRANULARITY) {
            return prob;
        }
        // we only compute the top Configuration.TRACKER_CUMULATIVE_TOPPERC percent for efficiency reason 
        double topNumber = currentTweetCount * Configuration.TRACKER_CUMULATIVE_TOPPERC;
        TDoubleIntMap copyOfScoreTracker;
        synchronized (predictScoreTracker) {
            copyOfScoreTracker = new TDoubleIntHashMap(predictScoreTracker);
        }
        TDoubleList scores = new TDoubleArrayList(copyOfScoreTracker.keys());
        while (scores.size() > 0) {
            double maxV = scores.max();
            cumulativeCount += copyOfScoreTracker.get(maxV);
            scores.remove(maxV);
            if (cumulativeCount >= topNumber || score >= maxV) {
                // for absolute score little than maxV, use the default value
                if (score >= maxV) {
                    prob = 1 - (double) cumulativeCount / currentTweetCount;
                }
                break;
            }
        }
        return prob;
    }

    private double trackPredictScore(TObjectDoubleMap<String> predictScores) {
        String[] scorenames = new String[]{Configuration.PRED_ABSOLUTESCORE};
        double[] scores = new double[scorenames.length];
        double absoluteScore = 0;
        for (int i = 0; i < scorenames.length; i++) {
            if (predictScores.containsKey(scorenames[i])) {
                scores[i] = predictScores.get(scorenames[i]);
                if (scorenames[i].equals(Configuration.PRED_ABSOLUTESCORE)) {
                    absoluteScore = (double) Math.round(scores[i] * Configuration.TRACKER_CUMULATIVE_GRANULARITY) / Configuration.TRACKER_CUMULATIVE_GRANULARITY;
                    synchronized (predictScoreTracker) {
                        predictScoreTracker.adjustOrPutValue(absoluteScore, 1, 1);
                    }
                }
            }
        }
        return absoluteScore;
    }

}
