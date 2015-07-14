package de.mpii.microblogtrack.component;

import de.mpii.microblogtrack.utility.Configuration;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
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
        URL u = new URL(url);
        HttpURLConnection huc = (HttpURLConnection) u.openConnection();
        huc.setRequestMethod("GET");
       // System.out.println(huc.);
        for (String key : huc.getHeaderFields().keySet()){
            System.out.println(key);
        }
        

//        try {
//            doc = Jsoup.connect(url).timeout(timeout).get();
//        } catch (Exception ex) {
//            // do nothing, just keep silence if the url is invalid
//        }
        return doc;
    }
    
    public static void main(String[] args) throws IOException{
        ExtractTweetText ett = new ExtractTweetText(100);
        ett.extracturl("http://stackoverflow.com/questions/11656064/how-to-get-page-meta-title-description-images-like-facebook-attach-url-using");
    }

    private String getUrlTitle(String url) throws IOException {
        Document doc = extracturl(url);
        String title = "";
        if (doc != null) {
            title = doc.title();
        }
        return title;
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
