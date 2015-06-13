package de.mpii.microblogtrack.component.filter;

import twitter4j.JSONObject;
import twitter4j.Status;

/**
 * interface for all possible filters
 *
 * @author khui
 */
public interface Filter {

    public boolean isRetain(String msg, JSONObject json, Status status);

}
