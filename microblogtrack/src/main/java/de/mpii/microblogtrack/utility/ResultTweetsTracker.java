package de.mpii.microblogtrack.utility;

import java.util.Collection;

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

    public void setCentroidNum(int centroidnum);

}
