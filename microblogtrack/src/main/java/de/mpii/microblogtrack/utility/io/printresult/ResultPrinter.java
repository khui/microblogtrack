package de.mpii.microblogtrack.utility.io.printresult;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 *
 * @author khui
 */
public class ResultPrinter {

    static Logger logger = Logger.getLogger(ResultPrinter.class.getName());

    private final Map<String, PrintStream> qidPs = new HashMap<>();

    private final String directory;

    public ResultPrinter(String outdir) throws FileNotFoundException {
        this.directory = outdir;
        File outd = new File(outdir);
        outd.deleteOnExit();
        outd.mkdirs();
        logger.info("Ouput directory " + outdir);
    }

    public void println(String qid, String line) throws FileNotFoundException {
        if (!qidPs.containsKey(qid)) {
            qidPs.put(qid, new PrintStream(new File(directory, qid)));
        }
        qidPs.get(qid).println(line);
    }

    public void close() {
        for (PrintStream ps : qidPs.values()) {
            ps.close();
        }
    }

    public void flush() {
        for (PrintStream ps : qidPs.values()) {
            ps.flush();
        }
    }

}
