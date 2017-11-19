package io.vertx.example.rxjava.http;

import io.vertx.example.util.Runner;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.http.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Single;
import rx.schedulers.Schedulers;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ThrottledServer extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(ThrottledServer.class);

    private AtomicInteger activeExecutions = new AtomicInteger(0);

    private int maxExecutionsPerInterval = 5;
    private int timeIntervalSeconds = 1;

    public static void main(String[] args) {
        Runner.runExample(ThrottledServer.class);
    }

    @Override
    public void start() throws Exception {
        HttpServer server = vertx.createHttpServer();
        server.requestStream().toObservable().subscribe(req -> {
            log.info("Got request");
            executeThrottled(doStuff())
                    // can use timeout to limit pressure
//                    .timeout(2, TimeUnit.SECONDS)
                    .subscribe(
                            responseText -> req.response().end(responseText),
                            throwable -> {
                                log.error(throwable.getMessage(), throwable);
                                req.response().setStatusCode(500).end(throwable.toString());
                            }
                    );
        });
        server.listen(8080);
    }

    public Single<String> doStuff() {
        return Single.just("Hey");
    }

    public <T> Single<T> executeThrottled(final Single<T> toExecute) {
        final AtomicInteger obtainedSession = new AtomicInteger();
        return Observable.defer(() -> Observable.just(activeExecutions.get() < maxExecutionsPerInterval))
                .filter(Boolean::booleanValue)
                .map(aBoolean -> activeExecutions.incrementAndGet())
                .doOnNext(obtainedSession::set)
                .repeatWhen(notification -> notification.takeWhile(aVoid -> obtainedSession.get() == 0)
                        .delay(timeIntervalSeconds * 1000 / (maxExecutionsPerInterval * 2), TimeUnit.MILLISECONDS)
                        .doOnNext(o -> log.debug("checking active execution count"))
                )
                .toSingle()
                .doOnEach(session -> log.debug("running with session " + session.getValue()))
                .flatMap(session -> toExecute)
                .doAfterTerminate(() -> Observable
                        .timer(timeIntervalSeconds, TimeUnit.SECONDS)
                        .doOnNext(aLong -> log.debug("returning session " + obtainedSession.get()))
                        .doOnNext(aLong -> activeExecutions.decrementAndGet())
                        .toSingle()
                        .map(o -> null)
                        .subscribeOn(Schedulers.trampoline())
                        .subscribe()
                );
    }

}
