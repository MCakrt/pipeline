package com.snapscore.pipeline.concurrency;

import com.snapscore.pipeline.logging.Logger;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Holds data that will be processed in parallel but also in a strictly sequential order defined by the specified
 * {@link InputQueueResolver}
 */
public class SequentialInput<I, R> {

    final I input;
    final InputQueueResolver<I> inputQueueResolver;
    final InputProcessingRunner<I, R> inputProcessingRunner;
    final LoggingInfo loggingInfo;

    /**
     * DEPRECATED use an appropriate newBuilder instead
     *
     * @param inputQueueResolver defines the ordering rules in which a piece of data will be processed
     */
    @Deprecated
    public SequentialInput(I input,
                           InputQueueResolver<I> inputQueueResolver,
                           InputProcessingRunner<I, R> inputProcessingRunner,
                           LoggingInfo loggingInfo) {
        this.input = input;
        this.inputQueueResolver = inputQueueResolver;
        this.inputProcessingRunner = inputProcessingRunner;
        this.loggingInfo = loggingInfo;
    }

    public static <I, R> BuilderForFlux<I, R> newBuilder(I input, InputQueueResolver<I> inputQueueResolver, Function<I, Flux<R>> processingFluxCreator) {
        return new BuilderForFlux<>(input, inputQueueResolver, processingFluxCreator);
    }

    public static <I, R> BuilderForCallable<I, R> newBuilder(I input, InputQueueResolver<I> inputQueueResolver, Callable<R> inputProcessing) {
        return new BuilderForCallable<>(input, inputQueueResolver, inputProcessing);
    }


    private static class Builder<I, R> {

        protected final I input;
        protected final InputQueueResolver<I> inputQueueResolver;
        protected final LoggingInfo.Builder loggingInfoBuilder = LoggingInfo.builder();

        private Builder(I input, InputQueueResolver<I> inputQueueResolver) {
            this.input = input;
            this.inputQueueResolver = inputQueueResolver;
        }

    }

    public static class BuilderForCallable<I, R> extends Builder<I, R> {

        private final Callable<R> inputProcessing;

        public BuilderForCallable(I input, InputQueueResolver<I> inputQueueResolver, Callable<R> inputProcessing) {
            super(input, inputQueueResolver);
            this.inputProcessing = inputProcessing;
        }

        public BuilderForCallable<I, R> setLogActivity(boolean logActivity) {
            this.loggingInfoBuilder.setLogActivity(logActivity);
            return this;
        }

        public BuilderForCallable<I, R> setInputLoggingDescription(String inputDescription) {
            this.loggingInfoBuilder.setInputDescription(inputDescription);
            this.setLogActivity(true);
            return this;
        }

        public BuilderForCallable<I, R> setLoggerDecorator(Function<Logger, Logger> loggerDecorator) {
            this.loggingInfoBuilder.setLoggerDecorator(loggerDecorator);
            this.setLogActivity(true);
            return this;
        }

        public SequentialInput<I, R> build() {
            final InputProcessingRunner<I, R> inputProcessingRunner;
            final LoggingInfo loggingInfo = super.loggingInfoBuilder.build();
            if (this.inputProcessing != null) {
                inputProcessingRunner = new InputProcessingCallableRunner<>(super.input, this.inputProcessing, loggingInfo);
            } else {
                throw new IllegalStateException("Cannot set inputProcessingRunner!");
            }

            return new SequentialInput<>(this.input, this.inputQueueResolver, inputProcessingRunner, loggingInfo);
        }

    }


    public static class BuilderForFlux<I, R> extends Builder<I, R> {

        private final Function<I, Flux<R>> processingFluxCreator;
        private Consumer<? super R> subscribeConsumer;
        private Consumer<? super Throwable> subscribeErrorConsumer;
        private Scheduler subscribeOnScheduler;

        public BuilderForFlux(I input, InputQueueResolver<I> inputQueueResolver, Function<I, Flux<R>> processingFluxCreator) {
            super(input, inputQueueResolver);
            this.processingFluxCreator = processingFluxCreator;
        }

        public BuilderForFlux<I, R> setSubscribeConsumer(Consumer<? super R> subscribeConsumer) {
            this.subscribeConsumer = subscribeConsumer;
            return this;
        }

        public BuilderForFlux<I, R> setSubscribeErrorConsumer(Consumer<? super Throwable> subscribeErrorConsumer) {
            this.subscribeErrorConsumer = subscribeErrorConsumer;
            return this;
        }

        public BuilderForFlux<I, R> setSubscribeOnScheduler(Scheduler subscribeOnScheduler) {
            this.subscribeOnScheduler = subscribeOnScheduler;
            return this;
        }

        public BuilderForFlux<I, R> setLogActivity(boolean logActivity) {
            this.loggingInfoBuilder.setLogActivity(logActivity);
            return this;
        }

        public BuilderForFlux<I, R> setInputLoggingDescription(String inputDescription) {
            this.loggingInfoBuilder.setInputDescription(inputDescription);
            this.setLogActivity(true);
            return this;
        }

        public BuilderForFlux<I, R> setLoggerDecorator(Function<Logger, Logger> loggerDecorator) {
            this.loggingInfoBuilder.setLoggerDecorator(loggerDecorator);
            this.setLogActivity(true);
            return this;
        }

        public SequentialInput<I, R> build() {
            final InputProcessingRunner<I, R> inputProcessingRunner;
            final LoggingInfo loggingInfo = this.loggingInfoBuilder.build();
            if (this.processingFluxCreator != null) {
                inputProcessingRunner = new InputProcessingFluxRunner<>(this.input, this.processingFluxCreator, this.subscribeConsumer, this.subscribeErrorConsumer, loggingInfo, this.subscribeOnScheduler);
            } else {
                throw new IllegalStateException("Cannot set inputProcessingRunner!");
            }

            return new SequentialInput<>(this.input, this.inputQueueResolver, inputProcessingRunner, loggingInfo);
        }

    }

}

