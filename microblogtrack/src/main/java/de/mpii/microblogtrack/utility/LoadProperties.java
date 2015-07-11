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
                    case "LUCENE_TOP_N_SEARCH":
                        Configuration.LUCENE_TOP_N_SEARCH = Integer.parseInt(value);
                        logger.info("LUCENE_TOP_N_SEARCH: " + value);
                        break;
                    case "DM_DIST_FILTER":
                        Configuration.DM_DIST_FILTER = Double.parseDouble(value);
                        logger.info("DM_DIST_FILTER: " + value);
                        break;
                    case "PW_DM_START_DELAY":
                        Configuration.PW_DM_START_DELAY = Integer.parseInt(value);
                        logger.info("PW_DM_START_DELAY: " + value);
                        break;
                    case "PW_DM_PERIOD":
                        Configuration.PW_DM_PERIOD = Integer.parseInt(value);
                        logger.info("PW_DM_PERIOD: " + value);
                        break;
                    case "PW_DW_CUMULATECOUNT_DELAY":
                        Configuration.PW_DW_CUMULATECOUNT_DELAY = Integer.parseInt(value);
                        logger.info("PW_DW_CUMULATECOUNT_DELAY: " + value);
                        break;
                    case "PW_DM_SELECTNUM":
                        Configuration.PW_DM_SELECTNUM = Integer.parseInt(value);
                        logger.info("PW_DM_SELECTNUM: " + value);
                        break;
                    case "LW_DM_START_DELAY":
                        Configuration.LW_DM_START_DELAY = Integer.parseInt(value);
                        logger.info("LW_DM_START_DELAY: " + value);
                        break;
                    case "LW_DM_PERIOD":
                        Configuration.LW_DM_PERIOD = Integer.parseInt(value);
                        logger.info("LW_DM_PERIOD: " + value);
                        break;
                    case "LW_DM_SELECTNUM":
                        Configuration.LW_DM_SELECTNUM = Integer.parseInt(value);
                        logger.info("LW_DM_SELECTNUM: " + value);
                        break;
                    case "TRACKER_CUMULATIVE_GRANULARITY":
                        Configuration.TRACKER_CUMULATIVE_GRANULARITY = Integer.parseInt(value);
                        logger.info("TRACKER_CUMULATIVE_GRANULARITY: " + value);
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
