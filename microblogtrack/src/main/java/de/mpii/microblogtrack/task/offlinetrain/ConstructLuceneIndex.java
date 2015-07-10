package de.mpii.microblogtrack.task.offlinetrain;

import de.mpii.microblogtrack.component.ExtractTweetText;
import de.mpii.microblogtrack.utility.Configuration;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

/**
 * read in off-line dumped tweets and generate lucene index to compute different
 * retrieval scores as features. Note that the analyzer, index settings should
 * be consistent with the online prediction task
 *
 * @author khui
 */
public class ConstructLuceneIndex {

    static Logger logger = Logger.getLogger(ConstructLuceneIndex.class.getName());

    private void write2Index(Status status, IndexWriter writer) throws IOException {
        ExtractTweetText textextractor = new ExtractTweetText();
        HashMap<String, String> fieldnameStr = new HashMap<>();
        String tweetstr = textextractor.getTweet(status);
        fieldnameStr.put(Configuration.TWEET_CONTENT, tweetstr);
        Document doc = new Document();
        doc.add(new LongField(Configuration.TWEET_ID, status.getId(), Field.Store.YES));
        for (String fieldname : fieldnameStr.keySet()) {
            doc.add(new TextField(fieldname, fieldnameStr.get(fieldname), Field.Store.YES));
        }
        writer.addDocument(doc);
    }

    private void ReadInTweets(String datadir, IndexWriter writer) {
        ZipFile zipf;
        String jsonstr;
        BufferedReader br;
        StringBuilder sb;
        File datasetDir = new File(datadir);
        for (File f : datasetDir.listFiles()) {
            if (f.getName().endsWith("zip")) {
                try {
                    zipf = new ZipFile(f);
                    Enumeration<? extends ZipEntry> entries = zipf.entries();
                    while (entries.hasMoreElements()) {
                        ZipEntry ze = (ZipEntry) entries.nextElement();
                        br = new BufferedReader(
                                new InputStreamReader(zipf.getInputStream(ze)));
                        sb = new StringBuilder();
                        while (br.ready()) {
                            sb.append(br.readLine());
                        }
                        jsonstr = sb.toString();
                        write2Index(TwitterObjectFactory.createStatus(jsonstr), writer);
                        br.close();
                    }
                    zipf.close();
                } catch (IOException | TwitterException ex) {
                    logger.error("readInTweets", ex);
                }
                logger.info("read in " + f.getName() + " finished");
            }
        }
        logger.info("finished read in all");
    }

    /**
     * read in tweet data and construct lucene index TODO: expand tweet with
     * embedding url landing page
     *
     * @param datadirs
     * @param indexdir
     * @throws java.io.IOException
     * @throws java.lang.ClassNotFoundException
     * @throws java.lang.InstantiationException
     * @throws java.lang.IllegalAccessException
     */
    public void ConstructIndex(String[] datadirs, String indexdir) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        Directory dir = FSDirectory.open(Paths.get(indexdir));
        Analyzer analyzer = (Analyzer) Class.forName(Configuration.LUCENE_ANALYZER).newInstance();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setRAMBufferSizeMB(Configuration.LUCENE_MEM_SIZE);
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (String datadir : datadirs) {
                ReadInTweets(datadir, writer);
                logger.info("Finished " + datadir);
            }
            writer.close();
        }
    }

    public static void main(String[] args) throws ParseException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        Options options = new Options();
        options.addOption("d", "datadirectory", true, "data directory");
        options.addOption("i", "indexdirectory", true, "index directory");
        options.addOption("l", "log4jxml", true, "log4j conf file");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        String datadirsBYCommas = null, indexdir = null, log4jconf = null;
        if (cmd.hasOption("i")) {
            indexdir = cmd.getOptionValue("i");
        }
        if (cmd.hasOption("d")) {
            datadirsBYCommas = cmd.getOptionValue("d");
        }
        if (cmd.hasOption("l")) {
            log4jconf = cmd.getOptionValue("l");
        }
        /**
         * for local test
         */
        String rootdir = "/home/khui/workspace/javaworkspace/twitter-localdebug";
        indexdir = rootdir + "/index";
        datadirsBYCommas = rootdir + "/tweetzip";
        log4jconf = "src/main/java/log4j.xml";

        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);
        logger.info("off-line training: construct index for off-line data");
        ConstructLuceneIndex trainer = new ConstructLuceneIndex();
        trainer.ConstructIndex(datadirsBYCommas.split(","), indexdir);
    }

}
