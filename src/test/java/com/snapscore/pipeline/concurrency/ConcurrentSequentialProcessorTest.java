package com.snapscore.pipeline.concurrency;

import com.snapscore.pipeline.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.snapscore.pipeline.concurrency.TestSupport.*;
import static org.junit.Assert.assertTrue;

public class ConcurrentSequentialProcessorTest {

    public static final int HEAVY_PROCESSING_MILLIS = 0;
    private final static Logger logger = Logger.setup(ConcurrentSequentialProcessorTest.class);
    private ExecutorService executorService;

    @Before
    public void setUp() throws Exception {
        executorService = Executors.newFixedThreadPool(8);
    }

    @After
    public void tearDown() throws Exception {
        executorService.shutdown();
    }

    @Test
    public void testThatMessagesOfSingleEntityAreProcessedSequentiallyAndInCorrectOrderInFluxBasedProcessing() throws Exception {

        // given
        final ConcurrentSequentialProcessor sequentialProcessor = new ConcurrentSequentialProcessorImpl("test-sequentialProcessor");
        final Map<Integer, TestMessage> prevProcessedTestMessageMap = new ConcurrentHashMap<>();
        final int entityCount = 1;
        final int messageCount = 10000;
        final AtomicBoolean correctOrder = new AtomicBoolean(true);

        final Consumer<TestMessage> assertion = m -> {
            boolean isCorrectNextMessage = prevProcessedTestMessageMap.get(m.entityId).messageNo + 1 == m.messageNo;
            prevProcessedTestMessageMap.put(m.entityId, m);
            if (!isCorrectNextMessage) {
                correctOrder.set(false);
            }
            assertTrue("Error in processed message order!", isCorrectNextMessage);
        };

        final List<SequentialInput<TestMessage, TestMessage>> sequentialInputData = createSequentialMessageFromFlux(prevProcessedTestMessageMap, entityCount, messageCount, assertion, this::processTestMessageFlux);

        // when
        sequentialInputData.forEach(sequentialProcessor::processSequentiallyAsync);

        sequentialProcessor.awaitProcessingCompletion(Duration.ofMillis(3000));

        // then
        assertTrue("Error in processed message order!", correctOrder.get());
    }

    @Test
    public void testThatMessagesOfMultipleEntitiesAreProcessedSequentiallyAndInCorrectOrderInFluxBasedProcessing() throws Exception {

        // given
        final ConcurrentSequentialProcessor sequentialProcessor = new ConcurrentSequentialProcessorImpl("test-sequentialProcessor");
        final Map<Integer, TestMessage> prevProcessedTestMessageMap = new ConcurrentHashMap<>();
        final int entityCount = 2 * Runtime.getRuntime().availableProcessors();
        final int messageCount = 30;
        final AtomicBoolean correctOrder = new AtomicBoolean(true);

        final Consumer<TestMessage> assertion = m -> {
            boolean isCorrectNextMessage = prevProcessedTestMessageMap.get(m.entityId).messageNo + 1 == m.messageNo;
            prevProcessedTestMessageMap.put(m.entityId, m);
            if (!isCorrectNextMessage) {
                correctOrder.set(false);
            }
            assertTrue("Error in processed message order!", isCorrectNextMessage);
        };

        final List<SequentialInput<TestMessage, TestMessage>> sequentialInputData = createSequentialMessageFromFlux(prevProcessedTestMessageMap, entityCount, messageCount, assertion, this::processTestMessageFlux);

        // when
        sequentialInputData.forEach(sequentialProcessor::processSequentiallyAsync);

        sequentialProcessor.awaitProcessingCompletion(Duration.ofMillis(20000));

        // then
        assertTrue("Error in processed message order!", correctOrder.get());
    }

    private Flux<TestMessage> processTestMessageFlux(TestMessage testMessage1) {
        return Flux.just(testMessage1)
                .doOnNext(testMessage0 -> logger.info("Before blocking processing {}", testMessage0))
                .publishOn(Schedulers.boundedElastic())
                .doOnNext(this::doBlockingOperation)
                .publishOn(Schedulers.parallel());
    }

    public List<SequentialInput<TestMessage, TestMessage>> createSequentialMessageFromFlux(Map<Integer, TestMessage> prevProcessedTestMessageMap,
                                                                                           int entityCount,
                                                                                           int messageCount,
                                                                                           Consumer<TestMessage> assertion,
                                                                                           Function<TestMessage, Flux<TestMessage>> processingFluxCreator) {
        IntStream.range(1, entityCount + 1).forEach(entityId -> {
            prevProcessedTestMessageMap.put(entityId, new TestMessage(entityId, 0));  // prepopulate with base message to ovoid NPE later ..
        });
        final List<SequentialInput<TestMessage, TestMessage>> sequentialInputs = new ArrayList<>();

        for (int messageNo = 1; messageNo <= messageCount; messageNo++) {
            for (int entityId = 1; entityId <= entityCount; entityId++) {
                TestMessage testMessage = new TestMessage(entityId, messageNo);
                SequentialInput<TestMessage, TestMessage> sequentialInput = SequentialInput.newBuilder(testMessage, new TestInputQueueResolver(), processingFluxCreator)
                        .setSubscribeOnScheduler(Schedulers.parallel())
                        .setSubscribeConsumer(m -> {
                            logger.info(" ---> Finished processing message: {}", m);
                            assertion.accept(m);
                        })
                        .setSubscribeErrorConsumer(e -> {
                            throw new RuntimeException(e);
                        })
                        .setInputLoggingDescription("entity id " + entityId)
                        .setLogActivity(false)
                        .build();
                sequentialInputs.add(sequentialInput);
            }
        }
        return sequentialInputs;
    }

    private void doBlockingOperation(TestMessage testMessage) {
        try {
            Thread.sleep(HEAVY_PROCESSING_MILLIS);
            logger.info("-> Running blocking operation of {}", testMessage);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testThatMessagesOfMultipleEntitiesAreProcessedSequentiallyAndInCorrectOrderInFutureBasedProcessing() throws Exception {

        // given
        final ConcurrentSequentialProcessor sequentialProcessor = new ConcurrentSequentialProcessorImpl("test-sequentialProcessor");
        final Map<Integer, TestMessage> prevProcessedTestMessageMap = new ConcurrentHashMap<>();
        final int entityCount = 10;
        final int messageCount = 1000;
        final AtomicBoolean correctOrder = new AtomicBoolean(true);

        final Consumer<TestMessage> assertion = m -> {
            boolean isCorrectNextMessage = prevProcessedTestMessageMap.get(m.entityId).messageNo + 1 == m.messageNo;
            prevProcessedTestMessageMap.put(m.entityId, m);
            if (!isCorrectNextMessage) {
                correctOrder.set(false);
            }
            assertTrue("Error in processed message order!", isCorrectNextMessage);
        };


        final List<SequentialInput<TestMessage, TestMessage>> sequentialInputData = createSequentialMessageFromFuture(prevProcessedTestMessageMap, entityCount, messageCount, assertion, this::processTestMessageCallable);

        // when
        sequentialInputData.forEach(sequentialProcessor::processSequentiallyAsync);

        sequentialProcessor.awaitProcessingCompletion(Duration.ofMillis(2000));

        // then
        assertTrue("Error in processed message order!", correctOrder.get());
    }

    public List<SequentialInput<TestMessage, TestMessage>> createSequentialMessageFromFuture(Map<Integer, TestMessage> prevProcessedTestMessageMap,
                                                                                             int entityCount,
                                                                                             int messageCount,
                                                                                             Consumer<TestMessage> assertion,
                                                                                             Function<TestMessage, Callable<TestMessage>> processingCallable) {
        IntStream.range(1, entityCount + 1).forEach(entityId -> {
            prevProcessedTestMessageMap.put(entityId, new TestMessage(entityId, 0));  // prepopulate with base message to ovoid NPE later ..
        });
        final List<SequentialInput<TestMessage, TestMessage>> sequentialInputs = new ArrayList<>();

        for (int messageNo = 1; messageNo <= messageCount; messageNo++) {
            for (int entityId = 1; entityId <= entityCount; entityId++) {
                final TestMessage testMessage = new TestMessage(entityId, messageNo);
                final Callable<TestMessage> callable = processingCallable
                        .compose(v -> {
                            assertion.accept(testMessage);
                            return testMessage;
                        }).apply(testMessage);
                final SequentialInput<TestMessage, TestMessage> sequentialInput = SequentialInput.newBuilder(testMessage, new TestInputQueueResolver(), callable)
                        .setLogActivity(false)
                        .build();
                sequentialInputs.add(sequentialInput);
            }
        }
        return sequentialInputs;
    }

    private Callable<TestMessage> processTestMessageCallable(TestMessage testMessage1) {
        return () -> {
            // inside the callable there can be a chain of async operations
            return CompletableFuture
                    .supplyAsync(() -> {
                        doBlockingOperation(testMessage1);
                        return testMessage1;
                    }, executorService)
                    .get();
        };
    }

}
