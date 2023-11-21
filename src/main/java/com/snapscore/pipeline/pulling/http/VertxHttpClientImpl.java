package com.snapscore.pipeline.pulling.http;

import com.snapscore.pipeline.logging.Logger;
import com.snapscore.pipeline.pulling.FeedPriorityEnum;
import com.snapscore.pipeline.pulling.FeedRequest;
import com.snapscore.pipeline.pulling.FeedRequestHttpHeader;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class VertxHttpClientImpl implements HttpClient {

    private final Logger logger = Logger.setup(VertxHttpClientImpl.class);

    private final Vertx vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(2));

    private final HttpClientConfig httpClientConfig;
    private final ClientCallbackFactory<VertxClientCallback> clientCallbackFactory;

    private final Map<FeedPriorityEnum, io.vertx.core.http.HttpClient> clients = new ConcurrentHashMap<>();

    public VertxHttpClientImpl(HttpClientConfig httpClientConfig,
                               HttpClientOptions httpClientOptions,
                               ClientCallbackFactory<VertxClientCallback> clientCallbackFactory) {
        this.httpClientConfig = httpClientConfig;
        this.clientCallbackFactory = clientCallbackFactory;
        for (FeedPriorityEnum requestPriority : FeedPriorityEnum.values()) {
            clients.put(requestPriority, vertx.createHttpClient(httpClientOptions));
        }
    }


    @Override
    public CompletableFuture<byte[]> getAsync(FeedRequest feedRequest) {

        Mono<byte[]> mono = Mono.create(emitter -> {

            VertxClientCallback clientCallback = clientCallbackFactory.createCallback(feedRequest, emitter);
            io.vertx.core.http.HttpClient client = clients.get(feedRequest.getPriority());

            RequestOptions requestOptions = new RequestOptions()
                    .setHost(httpClientConfig.host()) // TODO actually it might be good to have this as part of the request as well and if available then it should overwrite the general config value?
                    .setPort(httpClientConfig.port()) // TODO actually it might be good to have this as part of the request as well and if available then it should overwrite the general config value?
                    .setURI(feedRequest.getUrl())
                    .setTimeout(httpClientConfig.readTimeout().toMillis())
                    .setMethod(HttpMethod.GET);
            for (FeedRequestHttpHeader httpHeader : feedRequest.getHttpHeaders()) {
                requestOptions.addHeader(httpHeader.getKey(), httpHeader.getValue());
            }

            client.request(
                requestOptions,
                requestAsyncResult -> {
                    if (requestAsyncResult.succeeded()) {
                        HttpClientRequest requestSuccess = requestAsyncResult.result();
                        requestSuccess.send(responseAsyncResult -> {
                            if (responseAsyncResult.succeeded()) {
                                clientCallback.onResponse(responseAsyncResult.result());
                            } else {
                                clientCallback.handleException(requestAsyncResult.cause());
                            }
                        });
                    } else {
                        clientCallback.handleException(requestAsyncResult.cause());
                    }
                }
            );

            logger.decorateSetup(mdc -> mdc.analyticsId("http_client_got_accepted_rq")).info("HttpClient accepted new request: {}", feedRequest.toStringBasicInfo());

        });
        return mono.publishOn(Schedulers.parallel()) // emitted results need to be published on parallel scheduler so we do not execute pulled data processing on the httpClient's own threadpool
                .toFuture();

    }

    @Override
    public void shutdown() {
        for (io.vertx.core.http.HttpClient client : clients.values()) {
            client.close();
        }
    }

}
