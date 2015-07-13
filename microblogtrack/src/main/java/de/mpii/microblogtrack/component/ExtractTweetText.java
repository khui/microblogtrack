package de.mpii.microblogtrack.component;

import java.io.IOException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import twitter4j.Status;
import twitter4j.URLEntity;

/**
 *
 * @author khui
 */
public class ExtractTweetText {

    private Document doc = null;

    private void extracturl(String url) {
        if (doc == null) {
            try {
                doc = Jsoup.connect(url).get();
            } catch (Exception ex) {
                // do nothing, just keep silence if the url is invalid
            }
        }
    }

    private String getUrlTitle(String url) throws IOException {
        extracturl(url);
        String title = "";
        if (doc != null) {
            title = doc.title();
        }
        return title;
    }

    private String getUrlContent(String url) throws IOException {
        extracturl(url);
        String title = "";
        if (doc != null) {
            title = doc.body().text();
        }
        return title;
    }

    public String getTweet(Status status) {
        String cleanedtext = status.getText();
        return cleanedtext;
    }

    public String getUrlTitle(Status status) throws IOException {
        URLEntity[] urls = status.getURLEntities();
        StringBuilder sb = new StringBuilder();
        for (URLEntity url : urls) {
            sb.append(getUrlTitle(url.getURL())).append(" ");
        }
        return sb.toString();
    }

    public String getUrlContent(Status status) throws IOException {
        URLEntity[] urls = status.getURLEntities();
        StringBuilder sb = new StringBuilder();
        for (URLEntity url : urls) {
            sb.append(getUrlContent(url.getURL())).append(" ");
        }
        return sb.toString();
    }

    public String getExpanded(Status status) {
        String mergedtext;
        String tweettext = getTweet(status);
        String urltext = "";
        if (status.getURLEntities().length > 0) {
            urltext = getTweet(status);
        }
        mergedtext = String.join(" ", new String[]{tweettext, urltext});
        return mergedtext;
    }
}
