/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mpii.microblogtrack.listener.filter;

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
