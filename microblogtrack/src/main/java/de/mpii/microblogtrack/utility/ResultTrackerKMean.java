package de.mpii.microblogtrack.utility;

import com.google.common.collect.Lists;
import gnu.trove.TCollections;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.TDoubleIntMap;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TDoubleIntHashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

    // record the occrrence for each predict score, generating the approximating cumulative distribution
    private final TDoubleIntMap predictScoreTracker = TCollections.synchronizedMap(new TDoubleIntHashMap());

    // centroid number, default value is 10, governing how many classes we may have after clustering
    private int centroidnum = 10;
    // average distance among centroids, as reference in computing distance between two tweets
    private double avgCentroidDistance = 1;
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

    public ResultTrackerKMean(String queryid) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        this.queryid = queryid;
        this.distanceMeasure = (DistanceMeasure) Class.forName(MYConstants.DISTANT_MEASURE_CLUSTER).newInstance();
        // initialize an empty streamKMCentroids set
        this.streamKMCentroids = new ProjectionSearch(distanceMeasure, numProjections, searchSize);
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
        Vector v = qtp.vectorize();
        double absoluteScore = trackPredictScore(qtp.getPredictRes());
        // the unique predicting score for one tweet, and the corresponding cumulative prob is used as vector weight
        double relativeScore = getCumulativeProb(absoluteScore);
        // add the tweet to the clustering, using the tweet count as the centroid key
        synchronized (clusterer) {
            clusterer.cluster(new Centroid(tweetcount, v.clone(), relativeScore));
        }
        // update the average distance among centroids every 2 hours
        if (tweetcount % (MYConstants.TOP_N_FROM_LUCENE * 120) == 0) {
            try {
                updateAvgCentroidDist(this.centroidnum);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
                logger.error(ex.getMessage());
            }
        }
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

    private void updateAvgCentroidDist(int centroidnum) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        double distance = 0;
        double count = 1;
        Iterable<Vector> centroids = getCentroids(centroidnum);
        List<Vector> centroidsList = Lists.newArrayList(centroids);
        for (int i = 0; i < centroidsList.size(); i++) {
            for (int j = i + 1; j < centroidsList.size(); j++) {
                Vector c_x = centroidsList.get(i);
                Vector c_y = centroidsList.get(j);
                distance += distanceMeasure.distance(c_x, c_y);
                count++;
            }
        }
        avgCentroidDistance = distance / count;
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
        UpdatableSearcher emptySercher = new ProjectionSearch(distanceMeasure, numProjections, searchSize);
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

    /**
     * return the cumulative probability for the top
     * MYConstants.TRACKER_CUMULATIVE_TOPPERC tweets
     *
     * @param score
     * @return
     */
    private double getCumulativeProb(double score) {
        double prob = 0.5;
        int cumulativeCount = 0;
        // we only compute the top 10%
        double topNumber = this.tweetcount * MYConstants.TRACKER_CUMULATIVE_TOPPERC;
        TDoubleIntMap copyOfScoreTracker;
        synchronized (predictScoreTracker) {
            copyOfScoreTracker = new TDoubleIntHashMap(predictScoreTracker);
        }
        TDoubleList scores = new TDoubleArrayList(copyOfScoreTracker.keys());
        while (true) {
            double maxV = scores.max();
            cumulativeCount += copyOfScoreTracker.get(maxV);
            scores.remove(maxV);
            if (cumulativeCount >= topNumber || score >= maxV || scores.isEmpty()) {
                if (score < maxV) {
                    prob = 1 - MYConstants.TRACKER_CUMULATIVE_TOPPERC;
                } else {
                    prob = 1 - (double) cumulativeCount / this.tweetcount;
                }
                break;
            }
        }
        return prob;
    }

    private double trackPredictScore(TObjectDoubleMap<String> predictScores) {
        String[] scorenames = new String[]{MYConstants.PRED_ABSOLUTESCORE};
        double[] scores = new double[scorenames.length];
        double absoluteScore = 0;
        for (int i = 0; i < scorenames.length; i++) {
            if (predictScores.containsKey(scorenames[i])) {
                scores[i] = predictScores.get(scorenames[i]);
                if (scorenames[i].equals(MYConstants.PRED_ABSOLUTESCORE)) {
                    absoluteScore = (double) Math.round(scores[i] * 1000) / 1000;
                    synchronized (predictScoreTracker) {
                        predictScoreTracker.adjustOrPutValue(absoluteScore, 1, 1);
                    }
                }
            }
        }
        return absoluteScore;
    }

}
