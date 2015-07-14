package de.mpii.microblogtrack.component;

import de.mpii.microblogtrack.utility.CandidateTweet;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import info.debatty.java.stringsimilarity.Jaccard;
import info.debatty.java.stringsimilarity.interfaces.NormalizedStringSimilarity;
import org.apache.log4j.Logger;

/**
 *
 * @author khui
 */
public class TweetStringSimilarity implements TweetSimilarity {

    static Logger logger = Logger.getLogger(TweetStringSimilarity.class.getName());

    NormalizedStringSimilarity stringSimilarityComputor;

    public TweetStringSimilarity(int ngram) {
        stringSimilarityComputor = new Jaccard(ngram);
    }

    public TweetStringSimilarity() {
        this(0);
    }

    private double similarity(String content0, String content1, String urltitle0, String urltitle1) {
        double contentsimilarity = 0;
        double urltitlesimilarity = 0;
        double similarity = 0;
        try {
            contentsimilarity = stringSimilarityComputor.similarity(content0, content1);
            if (urltitle0 != null && urltitle1 != null) {
                urltitlesimilarity = stringSimilarityComputor.similarity(urltitle0, urltitle1);
            }
        } catch (Exception ex) {
            logger.error("", ex);
        }
        similarity = (contentsimilarity + urltitlesimilarity) / 2;
        return similarity;
    }

    @Override
    public double similarity(QueryTweetPair qtp0, CandidateTweet qtp1) {
        String content0 = qtp0.getTweetText();
        String urltitle0 = qtp0.getUrlTitleText();
        String content1 = qtp1.getTweetStr();
        String urltitle1 = qtp1.getUrlTitleText();
    }

    @Override
    public double similarity(CandidateTweet qtp0, CandidateTweet qtp1) {
    }

}
