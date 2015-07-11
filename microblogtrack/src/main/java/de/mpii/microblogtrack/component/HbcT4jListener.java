package de.mpii.microblogtrack.component;

import de.mpii.microblogtrack.component.core.LuceneScorer;
import java.io.IOException;
import org.apache.log4j.Logger;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

/**
 *
 * @author khui
 */
public class HbcT4jListener implements StatusListener {

    static Logger logger = Logger.getLogger(HbcT4jListener.class.getName());

    private final LuceneScorer lscorer;

    public HbcT4jListener(LuceneScorer lscorer) {
        this.lscorer = lscorer;
    }

    @Override
    public void onStatus(Status status) {
        try {
            lscorer.write2Index(status);
        } catch (IOException ex) {
            logger.error(ex.getMessage());
        }
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice sdn) {
    }

    @Override
    public void onTrackLimitationNotice(int limit) {
    }

    @Override
    public void onScrubGeo(long user, long upToStatus) {
    }

    @Override
    public void onStallWarning(StallWarning warning) {
    }

    @Override
    public void onException(Exception e) {
    }
}
