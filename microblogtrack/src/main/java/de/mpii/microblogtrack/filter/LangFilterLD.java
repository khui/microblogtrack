package de.mpii.microblogtrack.filter;

import twitter4j.JSONObject;
import twitter4j.Status;
import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import org.apache.log4j.Logger;

/**
 * this is language filter class based on language detector from
 * https://github.com/shuyo/language-detection
 *
 * @author khui
 */
public class LangFilterLD implements Filter {

    final Logger logger = Logger.getLogger(LangFilterLD.class);

    public static void loadprofile() throws LangDetectException {
        String profileDirectory = "/GW/D5data-2/khui/microblogtrack/lang-dect-profile";
        DetectorFactory.loadProfile(profileDirectory);
    }

    @Override
    public boolean isRetain(String msg, JSONObject json, Status status) {
        String profileDirectory = this.getClass().getClassLoader().getResource("lang-dect-profile").getFile();
        System.out.println(profileDirectory);
        try {
            DetectorFactory.loadProfile(profileDirectory);
            Detector detector = DetectorFactory.create();
            detector.append(status.getText());
            return detector.detect().equals("en");
        } catch (LangDetectException ex) {
            logger.error(ex.getCode() + ":" + ex.getMessage());
        }
        return false;
    }

    public String langdetect(String msg, JSONObject json, Status status) {
        String langtwitter = status.getLang();
        String errorstr = "";
        try {
            Detector detector = DetectorFactory.create();
            detector.append(status.getText());
            return detector.detect();
        } catch (LangDetectException ex) {
            errorstr = ex.getCode() + ":" + ex.getMessage();
            logger.error(errorstr);

        }
        return langtwitter;
    }

}
