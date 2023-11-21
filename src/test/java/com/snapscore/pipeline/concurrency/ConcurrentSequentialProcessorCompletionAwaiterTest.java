package com.snapscore.pipeline.concurrency;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static com.snapscore.pipeline.concurrency.TestSupport.*;
import static org.junit.Assert.*;

public class ConcurrentSequentialProcessorCompletionAwaiterTest {

    public static final int HEAVY_PROCESSING_MILLIS = 0;

    @Test
    public void emptyProcessorAreCompetedWithoutTimeout() {

        ConcurrentSequentialProcessor concurrentSequentialProcessor1 = new ConcurrentSequentialProcessorImpl("test-sequential-processor-1");
        ConcurrentSequentialProcessor concurrentSequentialProcessor2 = new ConcurrentSequentialProcessorImpl("test-sequential-processor-2");

        try {
            ConcurrentSequentialProcessorCompletionAwaiter.awaitProcessingCompletionOf(Set.of(concurrentSequentialProcessor1, concurrentSequentialProcessor2), Duration.ofMillis(1000));
        } catch (Exception e) {
            fail("awaiting processing completion should not have timed out");
        }
    }


    @Test
    public void independentProcessorsAreCompletedWithinTimeoutLimit() {

        ConcurrentSequentialProcessor concurrentSequentialProcessor1 = new ConcurrentSequentialProcessorImpl("test-sequential-processor-1");
        ConcurrentSequentialProcessor concurrentSequentialProcessor2 = new ConcurrentSequentialProcessorImpl("test-sequential-processor-2");

        final List<SequentialInput<TestMessage, TestMessage>> sequentialMessages = createSequentialMessage(10, 100, this::processTestMessageFlux);
        sequentialMessages.forEach(concurrentSequentialProcessor1::processSequentiallyAsync);
        sequentialMessages.forEach(concurrentSequentialProcessor2::processSequentiallyAsync);

        try {
            ConcurrentSequentialProcessorCompletionAwaiter.awaitProcessingCompletionOf(Set.of(concurrentSequentialProcessor1, concurrentSequentialProcessor2), Duration.ofMillis(2000));
        } catch (Exception e) {
            fail("awaiting processing completion should not have timed out");
        }

        assertEquals(0L, concurrentSequentialProcessor1.getTotalUnprocessedInputs());
        assertEquals(0L, concurrentSequentialProcessor2.getTotalUnprocessedInputs());
    }

    @Test(expected = TimeoutException.class)
    public void independentProcessorsAreTimedOut() throws Exception {

        ConcurrentSequentialProcessor concurrentSequentialProcessor1 = new ConcurrentSequentialProcessorImpl("test-sequential-processor-1");
        ConcurrentSequentialProcessor concurrentSequentialProcessor2 = new ConcurrentSequentialProcessorImpl("test-sequential-processor-2");

        final List<SequentialInput<TestMessage, TestMessage>> sequentialMessages = createSequentialMessage(10, 100, this::processTestMessageFlux);
        sequentialMessages.forEach(concurrentSequentialProcessor1::processSequentiallyAsync);
        sequentialMessages.forEach(concurrentSequentialProcessor2::processSequentiallyAsync);

        ConcurrentSequentialProcessorCompletionAwaiter.awaitProcessingCompletionOf(Set.of(concurrentSequentialProcessor1, concurrentSequentialProcessor2), Duration.ofMillis(10));
    }


    @Test
    public void interdependentProcessorsAreCompletedWithinTimeoutLimit() {

        ConcurrentSequentialProcessor concurrentSequentialProcessor1 = new ConcurrentSequentialProcessorImpl("test-sequential-processor-1");
        ConcurrentSequentialProcessor concurrentSequentialProcessor2 = new ConcurrentSequentialProcessorImpl("test-sequential-processor-2");

        final Function<TestMessage, Flux<TestMessage>> processingForProcessor1 = testMessage1 -> {
            return Flux.just(testMessage1)
                    .doOnNext(testMessage -> {
                        // processor one will forward all processing to processor 2
                        concurrentSequentialProcessor2.processSequentiallyAsync(createSequentialInput(this::processTestMessageFlux, testMessage.entityId, testMessage));
                    });
        };

        final List<SequentialInput<TestMessage, TestMessage>> sequentialMessages = createSequentialMessage(10, 100, processingForProcessor1);
        sequentialMessages.forEach(concurrentSequentialProcessor1::processSequentiallyAsync);

        try {
            ConcurrentSequentialProcessorCompletionAwaiter.awaitProcessingCompletionOf(Set.of(concurrentSequentialProcessor1, concurrentSequentialProcessor2), Duration.ofMillis(2000));
        } catch (Exception e) {
            fail("awaiting processing completion should not have timed out");
        }

        assertEquals(0L, concurrentSequentialProcessor1.getTotalUnprocessedInputs());
        assertEquals(0L, concurrentSequentialProcessor2.getTotalUnprocessedInputs());
    }


    @Test(expected = TimeoutException.class)
    public void interdependentProcessorsAreTimedOut() throws Exception {

        ConcurrentSequentialProcessor concurrentSequentialProcessor1 = new ConcurrentSequentialProcessorImpl("test-sequential-processor-1");
        ConcurrentSequentialProcessor concurrentSequentialProcessor2 = new ConcurrentSequentialProcessorImpl("test-sequential-processor-2");

        final Function<TestMessage, Flux<TestMessage>> processingForProcessor1 = testMessage1 -> {
            return Flux.just(testMessage1)
                    .doOnNext(testMessage -> {
                        // processor one will forward all processing to processor 2 ... making the processing dependent
                        concurrentSequentialProcessor2.processSequentiallyAsync(createSequentialInput(this::processTestMessageFlux, testMessage.entityId, testMessage));
                    });
        };

        final List<SequentialInput<TestMessage, TestMessage>> sequentialMessages = createSequentialMessage(10, 100, processingForProcessor1);
        sequentialMessages.forEach(concurrentSequentialProcessor1::processSequentiallyAsync);

        ConcurrentSequentialProcessorCompletionAwaiter.awaitProcessingCompletionOf(Set.of(concurrentSequentialProcessor1, concurrentSequentialProcessor2), Duration.ofMillis(10));

        Thread.sleep(100);
    }


    private Flux<TestMessage> processTestMessageFlux(TestMessage testMessage1) {
        return Flux.just(testMessage1)
                .publishOn(Schedulers.parallel())
                .doOnNext(testMessage2 -> doSomeHeavyProcessing(testMessage2))
                .publishOn(Schedulers.single())
                .doOnNext(testMessage2 -> doSomeHeavyProcessing(testMessage2))
                .publishOn(Schedulers.single());
    }

    public List<SequentialInput<TestMessage, TestMessage>> createSequentialMessage(int entityCount,
                                                                                   int messageCount,
                                                                                   Function<TestMessage, Flux<TestMessage>> processingFluxCreator) {

        final List<SequentialInput<TestMessage, TestMessage>> sequentialInputs = new ArrayList<>();

        for (int messageNo = 1; messageNo <= messageCount; messageNo++) {
            for (int entityId = 1; entityId <= entityCount; entityId++) {
                TestMessage testMessage = new TestMessage(entityId, messageNo);
                SequentialInput<TestMessage, TestMessage> sequentialInput = createSequentialInput(processingFluxCreator, entityId, testMessage);
                sequentialInputs.add(sequentialInput);
            }
        }
        return sequentialInputs;
    }

    private SequentialInput<TestMessage, TestMessage> createSequentialInput(Function<TestMessage, Flux<TestMessage>> processingFluxCreator,
                                                                            int entityId,
                                                                            TestMessage testMessage) {
        return SequentialInput.newBuilder(testMessage, new TestInputQueueResolver(), processingFluxCreator)
                .setInputLoggingDescription("entity id " + entityId)
                .setSubscribeOnScheduler(Schedulers.parallel())
                .setSubscribeErrorConsumer(e -> {
                    throw new RuntimeException(e);
                })
                .build();
    }

    private void doSomeHeavyProcessing(TestMessage testMessage) {
        try {
            Thread.sleep(HEAVY_PROCESSING_MILLIS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
