package de.mpii.microblogtrack.component.predictor;

import de.mpii.microblogtrack.utility.QueryTweetPair;

/**
 *
 * @author khui
 */
public interface PointwiseScorer {

    public abstract double predictor(QueryTweetPair qtr);
}
