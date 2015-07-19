package de.mpii.microblogtrack.utility;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.log4j.Logger;

/**
 *
 * @author khui
 */
public class LoadProperties {

    /**
     * including some parameters need to be set for pointwise decision maker:
     * PW_DM_START_DELAY PW_DM_PERIOD PW_DW_CUMULATECOUNT_DELAY for listwise
     * decision maker: LW_DM_START_DELAY LW_DM_PERIOD
     */
    static Logger logger = Logger.getLogger(LoadProperties.class.getName());

    public static void load(String propertyFile) {
        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream(propertyFile);
            prop.load(input);
            for (String key : prop.stringPropertyNames()) {
                String value = prop.getProperty(key);
                switch (key) {
                    //1
                    case "RUN_ID":
                        Configuration.RUN_ID = value;
                        logger.info("RUN_ID: " + value);
                        break;
                    //2
                    case "LISTENER_THREADNUM":
                        Configuration.LISTENER_THREADNUM = Integer.parseInt(value);
                        logger.info("LISTENER_THREADNUM: " + value);
                        break;
                    //3
                    case "LUCENE_SEARCH_THREADNUM":
                        Configuration.LUCENE_SEARCH_THREADNUM = Integer.parseInt(value);
                        logger.info("LUCENE_SEARCH_THREADNUM: " + value);
                        break;
                    //4
                    case "LUCENE_DOWNLOAD_URL_THREADNUM":
                        Configuration.LUCENE_DOWNLOAD_URL_THREADNUM = Integer.parseInt(value);
                        logger.info("LUCENE_DOWNLOAD_URL_THREADNUM: " + value);
                        break;
                    //5
                    case "LUCENE_DOWNLOAD_URL_TIMEOUT":
                        Configuration.LUCENE_DOWNLOAD_URL_TIMEOUT = Integer.parseInt(value);
                        logger.info("LUCENE_DOWNLOAD_URL_TIMEOUT: " + value);
                        break;
                    //6
                    case "QUERY_EXPANSION_TERMNUM":
                        Configuration.QUERY_EXPANSION_TERMNUM = Integer.parseInt(value);
                        logger.info("QUERY_EXPANSION_TERMNUM: " + value);
                        break;
                    //7
                    case "LUCENE_TOP_N_SEARCH":
                        Configuration.LUCENE_TOP_N_SEARCH = Integer.parseInt(value);
                        logger.info("LUCENE_TOP_N_SEARCH: " + value);
                        break;
                    //8
                    case "POINTWISE_PREDICTOR":
                        Configuration.POINTWISE_PREDICTOR = value;
                        logger.info("POINTWISE_PREDICTOR: " + value);
                        break;
                    //9
                    case "POINTWISE_PREDICTOR_COMBINE_ALPHA":
                        Configuration.POINTWISE_PREDICTOR_COMBINE_ALPHA = Double.parseDouble(value);
                        logger.info("POINTWISE_PREDICTOR_COMBINE_ALPHA: " + value);
                        break;
                    //10
                    case "POINTWISE_SVM_MODEL":
                        Configuration.POINTWISE_SVM_MODEL = value;
                        logger.info("POINTWISE_SVM_MODEL: " + value);
                        break;
                    //11
                    case "POINTWISE_SVM_SCALE":
                        Configuration.POINTWISE_SVM_SCALE = value;
                        logger.info("POINTWISE_SVM_SCALE: " + value);
                        break;
                    //12
                    case "DM_SIMILARITY_FILTER":
                        Configuration.DM_SIMILARITY_FILTER = Double.parseDouble(value);
                        logger.info("DM_DIST_FILTER: " + value);
                        break;
                    //13
                    case "PW_DM_SENT_QUEUETRACKER_LENLIMIT":
                        Configuration.PW_DM_SENT_QUEUETRACKER_LENLIMIT = Integer.parseInt(value);
                        logger.info("PW_DM_SENT_QUEUETRACKER_LENLIMIT: " + value);
                        break;
                    //14
                    case "DM_START_DELAY":
                        Configuration.DM_START_DELAY = Integer.parseInt(value);
                        logger.info("DM_START_DELAY: " + value);
                        break;
                    //15
                    case "PW_DM_PERIOD":
                        Configuration.PW_DM_PERIOD = Integer.parseInt(value);
                        logger.info("PW_DM_PERIOD: " + value);
                        break;
                    //16
                    case "PW_DW_CUMULATECOUNT_DELAY":
                        Configuration.PW_DW_CUMULATECOUNT_DELAY = Integer.parseInt(value);
                        logger.info("PW_DW_CUMULATECOUNT_DELAY: " + value);
                        break;
                    //17
                    case "LW_DM_SENT_QUEUETRACKER_LENLIMIT":
                        Configuration.LW_DM_SENT_QUEUETRACKER_LENLIMIT = Integer.parseInt(value);
                        logger.info("LW_DM_SENT_QUEUETRACKER_LENLIMIT: " + value);
                        break;
                    //18
                    case "LW_DM_PERIOD":
                        Configuration.LW_DM_PERIOD = Integer.parseInt(value);
                        logger.info("LW_DM_PERIOD: " + value);
                        break;
                    //19
                    case "LW_DM_QUEUE2PROCESS_LEN":
                        Configuration.LW_DM_QUEUE2PROCESS_LEN = Integer.parseInt(value);
                        logger.info("LW_DM_QUEUE_LEN: " + value);
                        break;
                    //20
                    case "LW_DM_METHOD":
                        Configuration.LW_DM_METHOD = value;
                        logger.info("LW_DM_METHOD: " + value);
                        break;
                    //21
                    case "MAXREP_SIMI_THRESHOLD":
                        Configuration.MAXREP_SIMI_THRESHOLD = Double.parseDouble(value);
                        logger.info("MAXREP_SIMI_THRESHOLD: " + value);
                        break;
                    //22
                    case "TRACKER_CUMULATIVE_GRANULARITY":
                        Configuration.TRACKER_CUMULATIVE_GRANULARITY = Integer.parseInt(value);
                        logger.info("TRACKER_CUMULATIVE_GRANULARITY: " + value);
                        break;
                    default:
                        logger.info("NO SUCH OPTION: " + key);
                        break;
                }
            }

        } catch (IOException ex) {
            logger.error("load property", ex);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException ex) {
                    logger.error("", ex);
                }
            }
        }

    }

}
