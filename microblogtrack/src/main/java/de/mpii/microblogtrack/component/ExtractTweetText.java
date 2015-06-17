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

    private String extracturl(String url) throws IOException {
        Document doc = Jsoup.connect(url).get();
        String title = doc.title().replaceAll("[^A-Za-z0-9]", " ");
        String text = doc.body().text().replaceAll("[^A-Za-z0-9]", " ");
        return title + " " + text;
    }

    public String getTweet(Status status) {
        String tweettext = status.getText();
        String cleanedtext = tweettext.replaceAll("[^A-Za-z0-9]", " ");
        return cleanedtext;
    }

    public String getUrl(Status status) throws IOException {
        URLEntity[] urls = status.getURLEntities();
        StringBuilder sb = new StringBuilder();
        for (URLEntity url : urls) {
            sb.append(extracturl(url.getURL())).append(" ");
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
