package com.snapscore.pipeline.pulling.http;

import com.snapscore.pipeline.logging.Logger;
import com.snapscore.pipeline.pulling.FeedRequest;
import io.vertx.core.http.HttpClientResponse;
import reactor.core.publisher.MonoSink;

import java.util.List;

public class VertxClientCallbackImpl extends AbstractClientCallback implements VertxClientCallback {

    private static final Logger logger = Logger.setup(VertxClientCallbackImpl.class);

    private final int httpResponseBufferSize;
    private final List<HeadersObserver> headersObservers;

    VertxClientCallbackImpl(PullingStatisticsService pullingStatisticsService,
                            FeedRequest feedRequest,
                            MonoSink<byte[]> emitter,
                            int httpResponseBufferSize,
                            List<HeadersObserver> headersObservers) {
        super(feedRequest, emitter, pullingStatisticsService);
        this.httpResponseBufferSize = httpResponseBufferSize;
        this.headersObservers = headersObservers == null ? List.of() : headersObservers;
    }

    VertxClientCallbackImpl(PullingStatisticsService pullingStatisticsService,
                           FeedRequest feedRequest,
                           MonoSink<byte[]> emitter,
                           int httpResponseBufferSize) {
        this(pullingStatisticsService, feedRequest, emitter, httpResponseBufferSize, List.of());
    }

    VertxClientCallbackImpl(FeedRequest feedRequest,
                            MonoSink<byte[]> emitter,
                            int httpResponseBufferSize) {
        this(null, feedRequest, emitter, httpResponseBufferSize);
    }

    @Override
    public void onResponse(HttpClientResponse response) {

        int statusCode = response.statusCode();

        try {
            if (statusCode >= 200 && statusCode <= 300) {
                logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid()).analyticsId("request_success")).info("Successful response for request: {}", feedRequest.toStringBasicInfo());
                response.bodyHandler(totalBuffer -> {
                    if (totalBuffer.getBytes().length == httpResponseBufferSize) {
                        logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).error("It's likely the receive buffer is not enough - currently set to: {} kbytes", httpResponseBufferSize / 1024);
                    }
                    byte[] responseData = totalBuffer.getBytes();
                    if (responseData != null) {
                        handleSuccessfulResponse(responseData);
                    } else {
                        logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).warn("Received responseBody is null!");
                        emitFailedRequestException();
                    }
                    pullingStatisticsService.ifPresent(service -> service.recordSuccessfulPullFor(feedRequest.getFeedName()));
                });
                response.exceptionHandler(throwable -> {
                    logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).error("Error while reading successful response for {}!", feedRequest.toStringBasicInfo(), throwable);
                    emitFailedRequestException();
                });
            } else {
                handleUnsuccessfulResponse(response.statusCode());
            }
            try {
                headersObservers.forEach(ho -> ho.observeHeaders(feedRequest.getFeedName(), response.headers()));
            } catch (Exception e) {
                logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).warn("Error while observing the response headers", e);
            }
        } catch (Exception e){
            logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).error("Error while processing response for: {}", feedRequest.toStringBasicInfo(), e);
            emitFailedRequestException();
        }
    }

    @Override
    public void handleException(Throwable e) {
        super.handleException(e);
    }


}
