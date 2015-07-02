package de.mpii.microblogtrack.utility.io.printresult;

import de.mpii.microblogtrack.utility.MYConstants;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 *
 * @author khui
 */
public class WriteTrecSubmission implements ResultPrinter {
    
    static Logger logger = Logger.getLogger(WriteTrecSubmission.class.getName());
    
    private final PrintStream ps;
    
    public WriteTrecSubmission(String outFile) throws FileNotFoundException {
        this.ps = new PrintStream(outFile);
        logger.info("The results will be printed to " + outFile);
    }
    
    @Override
    public void println(Map<String, String> fieldContent) {
        String queryId = fieldContent.get(MYConstants.QUERYID);
        String tweetId = fieldContent.get(MYConstants.TWEETID);
        String rank = fieldContent.get(MYConstants.RES_RANK);
        String methodId = fieldContent.get(MYConstants.RES_RUNINFO);
        String tweetstr = fieldContent.get(MYConstants.TWEETSTR);
        StringBuilder sb = new StringBuilder();
        sb.append(queryId).append(" ");
        sb.append("Q0").append(" ");
        sb.append(tweetId).append(" ");
        sb.append(rank).append(" ");
        sb.append(methodId).append("\t");
        sb.append(tweetstr);
        this.ps.println(sb.toString());
    }
    
    @Override
    public void close() {
        this.ps.close();
    }
    
    @Override
    public void flush() {
        this.ps.flush();
    }
    
}
