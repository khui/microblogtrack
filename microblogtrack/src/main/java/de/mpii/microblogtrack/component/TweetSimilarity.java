package de.mpii.microblogtrack.component;

import de.mpii.microblogtrack.utility.CandidateTweet;
import de.mpii.microblogtrack.utility.QueryTweetPair;

/**
 *
 * @author khui
 */
public interface TweetSimilarity {

    /**
     * compute the similarity between two tweets, the similarity should be
     * normalized between [0, 1]
     *
     * @param qtp0
     * @param qtp1
     * @return
     */
    public double similarity(QueryTweetPair qtp0, CandidateTweet qtp1);

    public double similarity(CandidateTweet qtp0, CandidateTweet qtp1);

}
