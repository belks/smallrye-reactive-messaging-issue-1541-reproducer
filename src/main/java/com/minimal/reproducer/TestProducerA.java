package com.minimal.reproducer;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Path("/test")
public class TestProducerA {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestProducerA.class);
    private static final ExecutorService executorService = Executors.newFixedThreadPool(100);
    private static final List<Future<?>> futures = new ArrayList<>();

    public static final byte[] TEST_DATA;
    static { // let's create a byte array that we can send around and compare for consistency
        int kb100 = 100 * 1024 * 1024;
        TEST_DATA = new byte[kb100];
        for (int i = 0; i<TEST_DATA.length; i++) {
            TEST_DATA[i] = (byte) (i % Byte.MAX_VALUE);
        }
    }

    private final Emitter<byte[]> emitter;

    @Inject
    public TestProducerA(@Channel("queue-A-out") @OnOverflow(value = OnOverflow.Strategy.NONE) Emitter<byte[]> emitter) {
        this.emitter = emitter;
    }

    @GET
    @Path("/start")
    @Produces(MediaType.TEXT_HTML)
    public String start(@QueryParam("count") int count) {
        LOGGER.info("Starting {} producer threads", count);
        for (int i = 0; i<count; i++) {
            futures.add(executorService.submit(new ProducerThread()));
        }
        return futures.size() + " running";
    }

    @GET
    @Path("/stop")
    @Produces(MediaType.TEXT_HTML)
    public String stopAll() {
        futures.forEach(future -> future.cancel(true));
        futures.clear();
        return "stopped";
    }

    private class ProducerThread implements Runnable {
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                emitter.send(TEST_DATA).toCompletableFuture().join();
            }
        }
    }

}
