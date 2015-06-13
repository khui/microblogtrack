package de.mpii.microblogtrack.archiver;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import de.mpii.microblogtrack.archiver.filewriter.Dump2Files;
import de.mpii.microblogtrack.component.filter.Filters;
import de.mpii.microblogtrack.archiver.listener.MultiKeysListenerHBC;
import de.mpii.microblogtrack.archiver.listener.MultiKeysListenerT4J;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * based on com.twitter.hbc.example.SampleStreamExample
 *
 * @author khui
 */
public class SampleMitBQ {

    final Logger logger = Logger.getLogger(SampleMitBQ.class);

    public void listenDump(String keydir, String outdir, int queueBound) throws IOException {
        // Create an appropriately sized blocking rawStreamQueue
        BlockingQueue<String> queue = new LinkedBlockingQueue<>(queueBound);
        ExecutorService listenerservice = Executors.newSingleThreadExecutor();
        ExecutorService dumperservice = Executors.newSingleThreadExecutor();
        listenerservice.submit(new MultiKeysListenerHBC(queue, keydir));
        dumperservice.submit(new Dump2Files(queue, outdir));
        while (true) {
            if (listenerservice.isShutdown()) {
                logger.warn("Listener stoped!");
            }
            if (dumperservice.isShutdown()) {
                logger.warn("Dumper stoped!");
            }
            if (queue.size() > queueBound * 0.9) {
                logger.warn("queue is almost full with: " + queue.size() + " items.");
            }
        }
    }

    public void listenFilterDump(String keydir, String outdir, int queueBound) throws IOException {
        int TIMEOUT = 60; // in seconds by default
        int numofthreads2filter = 2;
        // Create an appropriately sized blocking rawStreamQueue
        BlockingQueue<String> rawStreamQueue = new LinkedBlockingQueue<>(queueBound);
        BlockingQueue<String> filteredQueue = new LinkedBlockingQueue<>(queueBound);
        ExecutorService listenerservice = Executors.newSingleThreadExecutor();
        ExecutorService filterservice = Executors.newSingleThreadExecutor();
        ExecutorService dumperservice = Executors.newSingleThreadExecutor();

        listenerservice.submit(new MultiKeysListenerHBC(rawStreamQueue, keydir));
        filterservice.submit(new Filters(rawStreamQueue, filteredQueue, TIMEOUT, numofthreads2filter));
        dumperservice.submit(new Dump2Files(filteredQueue, outdir));
        while (true) {
            if (listenerservice.isShutdown()) {
                logger.warn("Listener stoped!");
            }
            if (dumperservice.isShutdown()) {
                logger.warn("Dumper stoped!");
            }
            if (filterservice.isShutdown()) {
                logger.warn("Filter stoped!");
            }
            if (rawStreamQueue.size() > queueBound * 0.9) {
                logger.warn("rawStreamQueue is almost full with: " + rawStreamQueue.size() + " items.");
            }
            if (filteredQueue.size() > queueBound * 0.9) {
                logger.warn("filteredQueue is almost full with: " + filteredQueue.size() + " items.");
            }
        }
    }

    public void multikeylistenerDumper(String keydir, String outdir, int queueBound) throws IOException {
        // Create an appropriately sized blocking rawStreamQueue
        BlockingQueue<String> rawStreamQueue = new LinkedBlockingQueue<>(queueBound);
        ExecutorService listenerservice = Executors.newSingleThreadExecutor();
        ExecutorService dumperservice = Executors.newSingleThreadExecutor();
        listenerservice.submit(new MultiKeysListenerT4J(rawStreamQueue, keydir));
        dumperservice.submit(new Dump2Files(rawStreamQueue, outdir));
    }

    public static void main(String[] args) throws ParseException, IOException {
        Logger.getRootLogger().setLevel(Level.INFO);
        Options options = new Options();
        options.addOption("o", "outdir", true, "output directory");
        options.addOption("k", "keydirectory", true, "api key directory");
        options.addOption("s", "boundsize", true, "bound size for the queque");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        String outputdir = null, keydir = null;
        int queueBound = 10000;
        if (cmd.hasOption("o")) {
            outputdir = cmd.getOptionValue("o");
        }
        if (cmd.hasOption("k")) {
            keydir = cmd.getOptionValue("k");
        }
        if (cmd.hasOption("s")) {
            queueBound = Integer.parseInt(cmd.getOptionValue("s"));
        }
        //new SampleMitBQ().listenFilterDump(keydir, outputdir, queueBound);
        new SampleMitBQ().multikeylistenerDumper(keydir, outputdir, queueBound);

    }

}
