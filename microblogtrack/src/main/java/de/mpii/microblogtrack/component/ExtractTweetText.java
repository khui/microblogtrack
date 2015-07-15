package de.mpii.microblogtrack.component;

import java.io.IOException;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import twitter4j.Status;
import twitter4j.URLEntity;

/**
 *
 * @author khui
 */
public class ExtractTweetText {

    static Logger logger = Logger.getLogger(ExtractTweetText.class.getName());

    private final int timeout;

    public ExtractTweetText(int timeout) {
        this.timeout = timeout;
    }

    private Document extracturl(String url) throws IOException {
        Document doc = null;
        try {
            doc = Jsoup.connect(url).timeout(timeout).get();
        } catch (Exception ex) {
            // do nothing, just keep silence if the url is invalid
        }
        return doc;
    }

    public String getUrlTitle(String url) throws IOException {
        Document doc = extracturl(url);
        String title = "";
        if (doc != null) {
            title = doc.title();
        }
        return title;
    }

    public TweetidUrl getUrlTitle(TweetidUrl tweetidUrl) throws IOException {
        Document doc = extracturl(tweetidUrl.url);
        String title;
        if (doc != null) {
            title = doc.title();
            tweetidUrl.urltitle = title;
            tweetidUrl.isAvailable = true;
        } else {
            tweetidUrl.isAvailable = false;
        }
        tweetidUrl.url = null;
        return tweetidUrl;
    }

    private String getUrlContent(String url) throws IOException {
        Document doc = extracturl(url);
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
        if (urls.length > 0) {
            // only crawl the first url's title
            URLEntity url = urls[0];
            sb.append(getUrlTitle(url.getURL()));
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

    public class TweetidUrl {

        public long tweetid;
        public String url;
        public String urltitle;
        public boolean isAvailable;
        public double similarity = -1;

        public TweetidUrl(long tweetid, String url) {
            this.url = url;
            this.tweetid = tweetid;
        }
    }
}
