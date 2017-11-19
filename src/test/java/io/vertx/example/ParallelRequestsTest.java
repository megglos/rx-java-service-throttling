package io.vertx.example;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.ext.web.client.HttpResponse;
import io.vertx.rxjava.ext.web.client.WebClient;
import io.vertx.rxjava.ext.web.codec.BodyCodec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import rx.Single;

import java.time.LocalTime;
import java.util.stream.IntStream;

@RunWith(VertxUnitRunner.class)
public class ParallelRequestsTest {

    Vertx vertx;

    @Before
    public void before(TestContext context) {
        vertx = Vertx.vertx();
    }

    @After
    public void after(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void parallelRequestsThrottleTest(TestContext context) {
        WebClient client = WebClient.create(vertx);
        Single<HttpResponse<String>> request = client.get(8080, "localhost", "/")
                .as(BodyCodec.string())
                .rxSend();

        int count = 40;
        Async asyncResponses = context.async(count);
        IntStream.range(0,count).parallel().forEach(value -> {
            request.subscribe(stringHttpResponse -> {
                context.assertEquals(stringHttpResponse.statusCode(), 200);
                System.out.println(LocalTime.now().toString() + " Request " + value + " got Response " + stringHttpResponse.statusCode());
                asyncResponses.countDown();
            });
        });
    }

}
