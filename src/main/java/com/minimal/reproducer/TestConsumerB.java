package com.minimal.reproducer;

import io.smallrye.reactive.messaging.annotations.Blocking;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import javax.enterprise.context.ApplicationScoped;
import java.util.Arrays;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class TestConsumerB {

    @Incoming("queue-B-in")
    @Blocking(value = "WorkerPool-B", ordered = false)
    public CompletionStage<Void> consume(Message<byte[]> message) {
        // process message
        if (!Arrays.equals(message.getPayload(), TestProducerA.TEST_DATA)) {
            throw new IllegalStateException();
        }

        return message.ack();
    }
}
