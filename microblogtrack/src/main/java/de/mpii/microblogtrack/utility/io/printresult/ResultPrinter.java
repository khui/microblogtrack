package de.mpii.microblogtrack.utility.io.printresult;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import org.apache.log4j.Logger;

/**
 *
 * @author khui
 */
public class ResultPrinter {

    static Logger logger = Logger.getLogger(ResultPrinter.class.getName());

    private final Map<String, PrintStream> qidPs = new HashMap<>();

    private final Map<String, PrintStream> qidlogPs = new HashMap<>();

    private final String directory;

    private final DecimalFormat df = new DecimalFormat("#.####");

    public ResultPrinter(String outdir) throws FileNotFoundException {
        this.directory = outdir;
        File outd = new File(outdir);
        outd.deleteOnExit();
        outd.mkdirs();
        logger.info("Ouput directory: " + outdir);
    }

    public String getTimeStamp() throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyMMdd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        String timestamp = dateFormat.parse(dateFormat.format(new Date())).toString();
        return timestamp;
    }

    public synchronized void printResult(String qid, String line) throws FileNotFoundException, ParseException {
        if (!qidPs.containsKey(qid)) {
            qidPs.put(qid, new PrintStream(new File(directory, qid + ".trec")));
        }
        qidPs.get(qid).println(line);
    }

    public synchronized void printlog(String qid, String tweet, String urlString, double absscore, double relscore) throws FileNotFoundException, ParseException {
        if (!qidlogPs.containsKey(qid)) {
            qidlogPs.put(qid, new PrintStream(new File(directory, qid + ".log")));
        }
        qidlogPs.get(qid).println(tweet + "\t" + urlString + "\t" + df.format(absscore) + "\t" + df.format(relscore) + "\t" + getTimeStamp());
        qidlogPs.get(qid).println();
    }

    public void close() {
        for (PrintStream ps : qidPs.values()) {
            ps.close();
        }
        for (PrintStream ps : qidlogPs.values()) {
            ps.close();
        }
    }

    public void flush() {
        for (PrintStream ps : qidPs.values()) {
            ps.flush();
        }
        for (PrintStream ps : qidlogPs.values()) {
            ps.flush();
        }
    }

}
