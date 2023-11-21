package com.snapscore.pipeline.concurrency;

/**
 * Defines the "grouping" of inputs that are to be processed so that all inputs that go into one group/queue
 * are processed in the same order in which they arrived in that group/queue.
 *
 * In other words the implementation of this determines what needs to be processed sequentially by passing the processed entity's ID
 *
 * Example:
 * When we want to process match data in parallel but also preserve the order for individual matches
 * we create an implementation of this base class that will take the matchId as the grouping key
 * so that all data for a single match gets processed in the order in which it was submitted for processing
 */
public abstract class InputQueueResolver<I> {

    /**
     * @param inputQueueCount total count of input queues provided by the queuing component
     * @see ConcurrentSequentialProcessorImpl
     */
    public abstract int getQueueIdxFor(I input, int inputQueueCount);

    protected int calcIdx(int inputQueueCount, String inputEntityIdentifier) {
        return Math.abs(inputEntityIdentifier.hashCode()) % inputQueueCount;
    }

    protected int calcIdx(int inputQueueCount, int inputEntityIdentifier) {
        return inputEntityIdentifier % inputQueueCount;
    }


}
