package com.snapscore.pipeline.concurrency;

import com.snapscore.pipeline.logging.Logger;
import reactor.util.annotation.Nullable;

import java.util.function.Function;

/**
 * Logging information that will be used by the internals of the data processing execution
 */
public class LoggingInfo {

    public final boolean logActivity;
    public final String inputDescription;
    @Nullable
    private final Function<Logger, Logger> loggerDecorator;

    @Deprecated // this will be made package private
    public LoggingInfo(boolean logActivity,
                       String inputDescription,
                       @Nullable Function<Logger, Logger> loggerDecorator) {
        this.logActivity = logActivity;
        this.inputDescription = inputDescription;
        this.loggerDecorator = loggerDecorator;
    }

    @Deprecated // this will be made package private
    public LoggingInfo(boolean logActivity, String inputDescription) {
        this(logActivity, inputDescription, null);
    }

    public Logger decorate(Logger logger) {
        if (loggerDecorator != null) {
            return loggerDecorator.apply(logger);
        } else {
            return logger;
        }
    }

    static Builder builder() {
        return new Builder();
    }

    static class Builder {

        public boolean logActivity;
        public String inputDescription;
        private Function<Logger, Logger> loggerDecorator;

        public Builder setLogActivity(boolean logActivity) {
            this.logActivity = logActivity;
            return this;
        }

        public Builder setInputDescription(String inputDescription) {
            this.inputDescription = inputDescription;
            return this;
        }

        public Builder setLoggerDecorator(Function<Logger, Logger> loggerDecorator) {
            this.loggerDecorator = loggerDecorator;
            return this;
        }

        public LoggingInfo build() {
            return new LoggingInfo(this.logActivity, this.inputDescription, this.loggerDecorator);
        }
    }

}
