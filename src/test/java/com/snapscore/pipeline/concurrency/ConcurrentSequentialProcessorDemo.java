package com.snapscore.pipeline.concurrency;

import com.snapscore.pipeline.logging.Logger;
import org.junit.Ignore;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.function.Function;

import static com.snapscore.pipeline.concurrency.TestSupport.TestInputQueueResolver;
import static com.snapscore.pipeline.concurrency.TestSupport.TestMessage;

public class ConcurrentSequentialProcessorDemo {

    private final static Logger logger = Logger.setup(ConcurrentSequentialProcessorDemo.class);

    /**
     * A demo of the functionality ... no asserts ... just comments and logs ...
     * @throws Exception
     */
    @Ignore
    @Test
    public void demoThatInputsAreBeingProcessedInCorrectOrderButAlsoInParallel() throws Exception {

        final ConcurrentSequentialProcessor sequentialProcessor = new ConcurrentSequentialProcessorImpl("test-sequentialProcessor");

        // two entoties
        for (int entityId = 0; entityId < 2; entityId++) {

            // number of message for each entity
            for (int messageNo = 0; messageNo < 10; messageNo++) {

                // data that we process
                final TestMessage testMessage = new TestMessage(entityId, messageNo);

                // logic that we want to run on the data
                final Function<TestMessage, Flux<TestMessage>> logicToRun = input -> Flux.just(input)
                        .doOnNext(i -> logger.info("STARTED: {}", testMessage))
                        .delayElements(Duration.ofMillis(500)) // introduce delay to the processing
                        .doOnComplete(() -> logger.info("FINISHED: {}", testMessage))
                        .subscribeOn(Schedulers.parallel());

                // this determines what gets processed sequentially
                final TestInputQueueResolver inputQueueResolver = new TestInputQueueResolver();

                // submit to the library for execution
                final SequentialInput<TestMessage, TestMessage> sequentialInput = SequentialInput.newBuilder(testMessage, inputQueueResolver, logicToRun).build();

                sequentialProcessor.processSequentiallyAsync(sequentialInput);

                // see in the logs that:
                // entities are processed in parallel in relation to each other but multiple messages for the same entity are procesed sequentially
            }
        }

        sequentialProcessor.awaitProcessingCompletion(Duration.ofMillis(10000));

    }

}
