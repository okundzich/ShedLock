package net.javacrumbs.shedlock.support;

import net.javacrumbs.shedlock.test.support.FuzzTester;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.time.Duration.ofMillis;

public class KeepAliveLockProviderFuzzTest {

    @Test
    void keepAliveLockProviderShouldPassFuzzTest() throws ExecutionException, InterruptedException {
        InMemoryLockProvider provider = new InMemoryLockProvider();
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(20);
        KeepAliveLockProvider keepAliveLockProvider = new KeepAliveLockProvider(provider, executorService);
        new FuzzTester(keepAliveLockProvider, ofMillis(75), ofMillis(50), 50).doFuzzTest();
    }
}
