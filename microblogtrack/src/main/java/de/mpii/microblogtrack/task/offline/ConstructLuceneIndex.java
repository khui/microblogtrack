package de.mpii.microblogtrack.task.offline;

import de.mpii.microblogtrack.component.ExtractTweetText;
import de.mpii.microblogtrack.utility.Configuration;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.IndexOptions;
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

    static final FieldType TEXT_OPTIONS = new FieldType();

    static {
        TEXT_OPTIONS.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        TEXT_OPTIONS.setStored(true);
        TEXT_OPTIONS.setTokenized(true);
    }

    /**
     * read in tweet data and construct lucene index TODO: expand tweet with
     * embedding url landing page
     *
     * @param datadirs
     * @param indexdir
     * @param threadnum
     * @throws java.io.IOException
     * @throws java.lang.ClassNotFoundException
     * @throws java.lang.InstantiationException
     * @throws java.lang.IllegalAccessException
     */
    public void constructIndex(String[] datadirs, String indexdir, int threadnum) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        Directory dir = FSDirectory.open(Paths.get(indexdir));
        Analyzer analyzer = (Analyzer) Class.forName(Configuration.LUCENE_ANALYZER).newInstance();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setRAMBufferSizeMB(Configuration.LUCENE_MEM_SIZE);
        File datasetDir;
        ExecutorService executor = Executors.newFixedThreadPool(threadnum);
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (String datadir : datadirs) {
                datasetDir = new File(datadir);
                for (File f : datasetDir.listFiles()) {
                    if (f.getName().endsWith("zip")) {
                        executor.execute(new AddTweet(writer, f));
                        logger.info("submitted " + f.getName() + ".");
                    }
                }
            }
            executor.shutdown();
            logger.info("wait until finished");
            // Wait until all threads are finish
            while (!executor.isTerminated()) {
            }
            writer.close();
        }

        logger.info("finished construct index");
    }

    private static class AddTweet implements Runnable {

        private final IndexWriter writer;

        private final File zipfile;

        AddTweet(IndexWriter writer, File zipfile) {
            this.writer = writer;
            this.zipfile = zipfile;
        }

        private void write2Index(File zipfile, IndexWriter writer) throws IOException, TwitterException {
            ZipFile zipf;
            String jsonstr;
            Status status;
            Document doc;
            String tweetstr, urltitle, urlcontent;
            BufferedReader br;
            StringBuilder sb;
            zipf = new ZipFile(zipfile);
            ExtractTweetText textextractor = new ExtractTweetText(Configuration.LUCENE_WRITE_JSOUP_TIMEOUT);
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
                status = TwitterObjectFactory.createStatus(jsonstr);
                tweetstr = textextractor.getTweet(status);
                urltitle = textextractor.getUrlTitle(status);
                urlcontent = textextractor.getUrlContent(status);
                if (!status.isRetweet()) {
                    doc = new Document();
                    doc.add(new LongField(Configuration.TWEET_ID, status.getId(), Field.Store.YES));
                    doc.add(new Field(Configuration.TWEET_CONTENT, tweetstr, TEXT_OPTIONS));
                    doc.add(new Field(Configuration.TWEET_URL_TITLE, urltitle, TEXT_OPTIONS));
                    doc.add(new Field(Configuration.TWEET_URL_CONTENT, urlcontent, TEXT_OPTIONS));
                    writer.addDocument(doc);
                }
                br.close();
            }
            zipf.close();
            logger.info(zipfile.getName() + " finished!");
        }

        @Override
        public void run() {

            try {
                write2Index(zipfile, writer);
            } catch (IOException | TwitterException ex) {
                logger.error("", ex);
            }
        }
    }

    public static void main(String[] args) throws ParseException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        Options options = new Options();
        options.addOption("d", "datadirectory", true, "data directory");
        options.addOption("i", "indexdirectory", true, "index directory");
        options.addOption("t", "threadnum", true, "threads number");
        options.addOption("l", "log4jxml", true, "log4j conf file");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        String datadirsBYCommas = null, indexdir = null, log4jconf = null;
        int threadnum = 16;
        if (cmd.hasOption("i")) {
            indexdir = cmd.getOptionValue("i");
        }
        if (cmd.hasOption("d")) {
            datadirsBYCommas = cmd.getOptionValue("d");
        }
        if (cmd.hasOption("t")) {
            threadnum = Integer.parseInt(cmd.getOptionValue("t"));
        }
        if (cmd.hasOption("l")) {
            log4jconf = cmd.getOptionValue("l");
        }
        /**
         * for local test
         */
//        String rootdir = "/home/khui/workspace/javaworkspace/twitter-localdebug";
//        indexdir = rootdir + "/index_url";
//        datadirsBYCommas = rootdir + "/tweetzip";
//        threadnum = 2;
//        log4jconf = "src/main/java/log4j.xml";

        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);
        logger.info("off-line training: construct index for off-line data");
        ConstructLuceneIndex trainer = new ConstructLuceneIndex();
        trainer.constructIndex(datadirsBYCommas.split(","), indexdir, threadnum);
    }

}
