package de.mpii.microblogtrack.component;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.log4j.Logger;

/**
 * called periodically. keep a single thread pool inside, in each run, it will
 * keep the existing task and submit the thread pool a new task, so that we
 * guarantee the decision maker will run exactly for one day
 *
 * @author khui
 */
public class DecisionMakerTimer implements Runnable {

    static Logger logger = Logger.getLogger(DecisionMakerTimer.class.getName());

    private ScheduledExecutorService excutor = Executors.newScheduledThreadPool(1);

    private final PointwiseDecisionMaker decisionmaker;

    private Future<?> lastExecution = null;

    public DecisionMakerTimer(PointwiseDecisionMaker decisionmaker) {
        this.decisionmaker = decisionmaker;
    }

    @Override
    public void run() {

        if (lastExecution != null) {
            if (!lastExecution.isDone()) {
                boolean interrupted = lastExecution.cancel(true);
                if (interrupted) {
                    logger.warn("latest decision maker has been canceled due to its overtime.");
                } else {
                    logger.error("cancel job failed, now reboot the executor.");
                    excutor.shutdownNow();
                    excutor = Executors.newScheduledThreadPool(1);
                }
            }
        }
        lastExecution = excutor.submit(decisionmaker);
    }

}
