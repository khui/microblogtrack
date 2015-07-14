package de.mpii.microblogtrack.component;

import de.mpii.microblogtrack.utility.Configuration;
import de.mpii.microblogtrack.utility.QueryTweetPair;
import info.debatty.java.stringsimilarity.Jaccard;
import info.debatty.java.stringsimilarity.NormalizedLevenshtein;
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
        stringSimilarityComputor = new NormalizedLevenshtein();
    }

    public TweetStringSimilarity() {
        // by default we compute unigram similarity
        this(1);
    }

    private double similarity(String content0, String content1, String urltitle0, String urltitle1) {
        double contentsimilarity = 0;
        double contentlen = content0.length() + content1.length() + 0.9;
        double urltitlelen = 0.1;
        double urltitlesimilarity = 0;
        double similarity;
        try {
            contentsimilarity = stringSimilarityComputor.similarity(content0, content1);
            if (urltitle0 != null && urltitle1 != null) {
                urltitlelen += urltitle0.length() + urltitle1.length();
                urltitlesimilarity = stringSimilarityComputor.similarity(urltitle0, urltitle1);
            }
        } catch (Exception ex) {
            logger.error("", ex);
        }
        double totallen = contentlen + urltitlelen;
        similarity = (contentlen / totallen) * contentsimilarity + (urltitlelen / totallen) * urltitlesimilarity;
        if (similarity > Configuration.DM_SIMILARITY_FILTER) {
            logger.info(contentsimilarity + "\t" + urltitlesimilarity + "\t" + content0 + "\t" + content1 + "\t" + urltitle0 + "\t" + urltitle1);
        }
        return similarity;
    }

    @Override
    public double similarity(QueryTweetPair qtp0, QueryTweetPair qtp1) {
        String content0 = qtp0.getTweetText();
        String urltitle0 = qtp0.getUrlTitleText();
        String content1 = qtp1.getTweetText();
        String urltitle1 = qtp1.getUrlTitleText();
        return similarity(content0, content1, urltitle0, urltitle1);
    }

}
