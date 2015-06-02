/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mpii.microblogtrack.listener.filter;

import com.twitter.hbc.twitter4j.parser.JSONObjectParser;
import java.io.IOException;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import twitter4j.JSONException;
import twitter4j.JSONObject;
import twitter4j.JSONObjectType;
import twitter4j.PublicObjectFactory;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.conf.ConfigurationBuilder;

/**
 * filter the stream and only retain the status this has to be the first filter
 * being used as long as the tweets are solely of interest among all types of
 * stream
 *
 * thereby, follow-up filters can read status, tweetid and json object to save
 * computation
 *
 * @author khui
 */
public class StatusFilter implements Filter {

    final Logger logger = Logger.getLogger(StatusFilter.class);

    private JSONObject json = null;

    private Status status = null;

    private long tweetid = -1;

    private final PublicObjectFactory factory = new PublicObjectFactory(new ConfigurationBuilder().build());

    /**
     *
     * @param msg
     * @param json = null
     * @param status = null
     * @return
     */
    @Override
    public boolean isRetain(String msg, JSONObject json, Status status) {
        try {
            isStatus(msg);
        } catch (JSONException | TwitterException | IOException ex) {
            java.util.logging.Logger.getLogger(StatusFilter.class.getName()).log(Level.SEVERE, null, ex);
        }
        boolean isstatus = false;
        if (tweetid > 0) {
            isstatus = true;
        }
        return isstatus;
    }

    public Status getStatus() {
        return status;
    }

    public JSONObject getJson() {
        return json;
    }

    public long getTweetid() {
        return tweetid;
    }

    /**
     * referred com.twitter.hbc.twitter4j.BaseTwitter4jClient given an input
     * stream, check whether it is status or not currently only response to
     * status/restatus
     *
     * @param msg
     * @return
     * @throws JSONException
     * @throws TwitterException
     * @throws IOException
     */
    private void isStatus(String msg) throws JSONException, TwitterException, IOException {
        json = new JSONObject(msg);
        JSONObjectType.Type type = JSONObjectType.determine(json);
        int statustype = 0;
        switch (type) {
            case STATUS:
                statustype = 1;
                break;
            default:
                // sole RT?
                if (JSONObjectParser.isRetweetMessage(json)) {
                    statustype = 2;
                }
                break;
        }
        try {
            switch (statustype) {
                case 1:
                    status = factory.createStatus(json);
                    break;
                case 2:
                    status = factory.createStatus(JSONObjectParser.parseEventTargetObject(json));
                    logger.error(type + "\n" + msg + "\n\n");
                    break;
            }
        } catch (TwitterException | JSONException e) {
            logger.error(e.getMessage() + "\n" + msg + "\n\n");
        }
        if (status != null) {
            tweetid = status.getId();
        }
    }
}
