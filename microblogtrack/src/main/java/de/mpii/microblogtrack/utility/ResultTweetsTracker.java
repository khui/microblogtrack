package de.mpii.microblogtrack.utility;

import java.util.Collection;
import java.util.Map;

/**
 *
 * @author khui
 */
public interface ResultTweetsTracker {

    /**
     * add a tweet to this query profile: add features, add scores from the
     * pointwise prediction procedure. note that both the features and the
     * prediction scores can be multi dimensions. The example for the latter is,
     * the outcome of the svm prediction, normally including confidence, the
     * distance to the hyperplane etc.. Meanwhile, we update the tracking
     * centroids
     *
     *
     * @param qtps
     */
    public void addTweets(Collection<QueryTweetPair> qtps);

    /**
     * return the average distance among different centroids at this moment
     *
     * @return
     */
    public double avgDistCentroids();

    /**
     * return relative score between 0..1
     *
     * @param absoluteScore
     * @return
     */
    public double relativeScore(double absoluteScore);

    /**
     * explicitly set the centroid numbers. the default value is 10, due to at
     * most 10 tweets are requested per day for the notification task
     *
     * @param centroidnum
     */
    public void setCentroidNum(int centroidnum);

    /**
     * return whether the decision maker has started, since we are only
     * interested in latest tweets this it to inform the lucenescore component
     * whether to record the tweet in queue
     *
     * @return
     */
    public boolean isStarted();

    /**
     * inform that we can start to make decision of the input tweet
     */
    public void informStart2Record();

    /**
     * return the min/max value for each feature. note that this function is
     * supposed to be concurrently called, thus thread-safe implementation is
     * preferred
     *
     * @return
     */
    public Map<String, double[]> getMeanStdScaler();

    /**
     * for each query tweet pair, update the min/max tracker, this also prefers
     * a high concurrency implementation
     *
     * @param qtp
     */
    //public void updateFeatureMinMax(QueryTweetPair qtp);
}
