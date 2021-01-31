package com.snapscore.pipeline.utils.reactive.sequentialisation;

import com.snapscore.pipeline.logging.Logger;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


public class SequentialFluxProcessorImpl implements SequentialFluxProcessor {

    private static final Logger logger = Logger.setup(SequentialFluxProcessorImpl.class);

    private static final int INPUT_QUEUES_COUNT_DEFAULT = 100000;
    public static final String UNPROCESSED_TOTAL_LOG_ANALYTICS_ID = "unprocessed_total";
    private final int inputQueueCount;

    // as we need to ensure that access to the message queues is atomic we need to lock on this object
    private final Object queueLock = new Object();
    // guarded by "queueLock"
    private final Map<Integer, Queue<EnqueuedInput>> inputQueues = new ConcurrentHashMap<>();
    // guarded by "queueLock"
    private final AtomicLong totalEnqueuedInputs = new AtomicLong(0);

    /**
     * @param inputQueueCount should be a big enough number for the passed messages to get spread out evenly
     */
    public SequentialFluxProcessorImpl(int inputQueueCount) {
        this.inputQueueCount = inputQueueCount;
        for (int queueIdx = 0; queueIdx < this.inputQueueCount; queueIdx++) {
            inputQueues.put(queueIdx, new LinkedList<>());
        }
    }

    public SequentialFluxProcessorImpl() {
        this(INPUT_QUEUES_COUNT_DEFAULT);
    }

    @Override
    public <I, R> void processSequentially(SequentialInput<I, R> sequentialInput) {
        int queueIdx = sequentialInput.queueResolver.getQueueIdxFor(sequentialInput.input, inputQueueCount);
        EnqueuedInput enqueuedInput = new EnqueuedInput(queueIdx, sequentialInput.input, sequentialInput.sequentialFluxSubscriber, sequentialInput.loggingInfo);
        enqueueAndProcess(enqueuedInput);
    }

    private void enqueueAndProcess(EnqueuedInput enqueuedInput) {
        boolean canProcessImmediately;
        Logger loggerDecorated = enqueuedInput.loggingInfo.decorate(logger);
        int queueIdx = enqueuedInput.queueIdx;
        int queueSize;
        synchronized (queueLock) {
            Queue<EnqueuedInput> queue = inputQueues.get(queueIdx);
            if (queue == null) {
                loggerDecorated.error("Failed to find queue for queue no. {}", queueIdx);
                queue = new LinkedList<>();
                inputQueues.put(queueIdx, queue);
            }
            canProcessImmediately = queue.isEmpty();
            queue.add(enqueuedInput);
            queueSize = queue.size();
            totalEnqueuedInputs.incrementAndGet();
        }
        loggerDecorated.decorateSetup(props -> props.analyticsId(UNPROCESSED_TOTAL_LOG_ANALYTICS_ID).exec(String.valueOf(totalEnqueuedInputs.get())))
                .info("Input queue no. {} size = {}; Enqueued inputs total = {}. Just enqueued input {}", queueIdx, queueSize, totalEnqueuedInputs.get(), enqueuedInput.loggingInfo.getMessage());
        loggerDecorated.info("canProcessImmediately = {} for input {}", canProcessImmediately, enqueuedInput.loggingInfo.getMessage());
        if (canProcessImmediately) {
            // if no previous item is being processed then we can send this one immediately
            processNext(enqueuedInput);
        }
    }

    private void processNext(EnqueuedInput enqueuedInput) {
        Logger loggerDecorated = enqueuedInput.loggingInfo.decorate(logger);
        loggerDecorated.info("Going to process next input: {}", enqueuedInput.loggingInfo.getMessage());
        logIfWaitingForTooLong(enqueuedInput);
        enqueuedInput.sequentialFluxSubscriber.subscribe( // Subscribing with these hoods is EXTREMELY important to ensure that the next message is taken from the queue and processed
                () -> dequeueCurrentAndProcessNext(enqueuedInput),
                () -> dequeueCurrentAndProcessNext(enqueuedInput),
                enqueuedInput.enqueuedTs
        );
    }

    private void dequeueCurrentAndProcessNext(EnqueuedInput currInput) {
        Logger loggerDecorated = currInput.loggingInfo.decorate(logger);
        try {
            loggerDecorated.info("Entered dequeueCurrentAndProcessNext after finished processing input: {}", currInput.loggingInfo.getMessage());
            EnqueuedInput nextInput;
            int newQueueSize;
            int queueIdx = currInput.queueIdx;
            synchronized (queueLock) {
                Queue<EnqueuedInput> queue = inputQueues.get(queueIdx);
                queue.poll(); // dequeue the previously processed item
                totalEnqueuedInputs.decrementAndGet();
                newQueueSize = queue.size();
                nextInput = queue.peek();
            }
            loggerDecorated.decorateSetup(props -> props.analyticsId(UNPROCESSED_TOTAL_LOG_ANALYTICS_ID).exec(String.valueOf(totalEnqueuedInputs.get())))
                    .info("Input queue no. {} size = {}; Enqueued inputs total = {}. ... after polling last processed input: {}", queueIdx, newQueueSize, totalEnqueuedInputs.get(), currInput.loggingInfo.getMessage());
            if (nextInput != null) {
                processNext(nextInput);
            }
        } catch (Exception e) {
            loggerDecorated.error("Error inside dequeueCurrentAndProcessNext! {}", currInput.loggingInfo.getMessage());
        }
    }

    private void logIfWaitingForTooLong(EnqueuedInput input) {
        long waitingMillis = System.currentTimeMillis() - input.enqueuedTs;
        if (waitingMillis > 2_000) {
            logger.decorateSetup(mdc -> mdc.analyticsId("enqueued_input_for_too_long")).warn("EnqueuedInput waiting too long for processing: {} ms; Enqueued inputs total = {}; input: {}", waitingMillis, totalEnqueuedInputs.get(), input.loggingInfo.getMessage());
        }
    }


    private static class EnqueuedInput {

        private final int queueIdx;
        private final Object inputData;
        private final SequentialFluxSubscriber<?, ?> sequentialFluxSubscriber;
        private final LoggingInfo loggingInfo;
        private final long enqueuedTs;

        public EnqueuedInput(int queueIdx,
                             Object inputData,
                             SequentialFluxSubscriber<?, ?> sequentialFluxSubscriber,
                             LoggingInfo loggingInfo) {
            this.queueIdx = queueIdx;
            this.inputData = inputData;
            this.sequentialFluxSubscriber = sequentialFluxSubscriber;
            this.loggingInfo = loggingInfo;
            this.enqueuedTs = System.currentTimeMillis();
        }
    }


}
