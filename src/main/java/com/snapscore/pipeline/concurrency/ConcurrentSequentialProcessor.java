package com.snapscore.pipeline.concurrency;

import java.time.Duration;

/**
 * Makes it possible to process any data in full parallel mode but also preserving order defined.
 *
 * In other words: Entities/inputs are processed in parallel in relation to each other but multiple inputs for the same entity are processed sequentially
 * in the exact order in which they were submitted for processing
 * An Entity's ID is most suitable for distinguishing between them and this is achieved by subclassing {@link InputQueueResolver} for a specific tpe of entity we process
 * by each {@link SequentialInput}. Internally this decides what must be processed sequentially.
 *
 */
public interface ConcurrentSequentialProcessor {

    <I, R> void processSequentiallyAsync(SequentialInput<I, R> sequentialInput);

    void awaitProcessingCompletion(Duration timeout) throws Exception;

    long getTotalUnprocessedInputs();
}
