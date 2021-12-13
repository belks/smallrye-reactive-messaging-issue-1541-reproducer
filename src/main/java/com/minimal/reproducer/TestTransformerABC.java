package com.minimal.reproducer;


import io.smallrye.reactive.messaging.annotations.Blocking;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.concurrent.CompletionStage;

public class TestTransformerABC {

    private final Emitter<byte[]> emitterQueueB;
    private final Emitter<byte[]> emitterQueueC;

    @Inject
    public TestTransformerABC(
            @Channel("queue-B-out") @OnOverflow(value = OnOverflow.Strategy.NONE) Emitter<byte[]> emitterQueueB,
            @Channel("queue-C-out") @OnOverflow(value = OnOverflow.Strategy.NONE) Emitter<byte[]> emitterQueueC) {
        this.emitterQueueB = emitterQueueB;
        this.emitterQueueC = emitterQueueC;
    }

    @Incoming("queue-A-in")
    @Blocking(value = "WorkerPool-A", ordered = false)
    public CompletionStage<Void> transform(Message<byte[]> message) {
        // process message
        byte[] messageData = message.getPayload();

        if (!Arrays.equals(messageData, TestProducerA.TEST_DATA)) {
            throw new IllegalStateException();
        }

        CompletionStage<Void> firstMsg = emitterQueueB.send(messageData);
        CompletionStage<Void> secondMsg = emitterQueueC.send(messageData);
        firstMsg.thenCombine(secondMsg, (void1, void2) -> null).toCompletableFuture().join();
        return message.ack();
    }

}
