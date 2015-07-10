package de.mpii.microblogtrack.task.expansion;

import cc.wikitools.lucene.IndexWikipediaDump;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 *
 * @author khui
 */
public class BuildWikiIndex {

    static Logger logger = Logger.getLogger(BuildWikiIndex.class.getName());

    public static void main(String[] args) throws org.apache.commons.cli.ParseException, IOException, UnsupportedEncodingException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        Options options = new Options();
        options.addOption("f", "wikifile", true, "wiki xml file");
        options.addOption("i", "indexdirectory", true, "index directory");
        options.addOption("l", "log4jxml", true, "log4j conf file");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        String wikifile = null, indexdir = null, log4jconf = null;

        if (cmd.hasOption("l")) {
            log4jconf = cmd.getOptionValue("l");
        }
        if (cmd.hasOption("f")) {
            wikifile = cmd.getOptionValue("f");
        }
        if (cmd.hasOption("i")) {
            indexdir = cmd.getOptionValue("i");
        }

        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);
        logger.info("construct wiki lucene index.");
        IndexWikipediaDump.constructIndex(indexdir, wikifile);

    }

}
