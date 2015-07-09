package de.mpii.microblogtrack.component;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

    private final ScheduledExecutorService startExecutor;

    private final Runnable decisionmaker;

    private Future<?> lastExecution = null;

    private final String taskname;

    public DecisionMakerTimer(Runnable decisionmaker, String taskname, int threadnum) {
        this.decisionmaker = decisionmaker;
        this.startExecutor = Executors.newScheduledThreadPool(threadnum);
        this.taskname = taskname;
    }

    @Override
    public void run() {
        if (lastExecution != null) {
            if (!lastExecution.isDone()) {
                boolean interrupted = lastExecution.cancel(true);
                if (interrupted) {
                    logger.warn("interrupte flag has been set for " + this.taskname);
                }
            }
        }
        lastExecution = startExecutor.submit(decisionmaker);
    }

}
