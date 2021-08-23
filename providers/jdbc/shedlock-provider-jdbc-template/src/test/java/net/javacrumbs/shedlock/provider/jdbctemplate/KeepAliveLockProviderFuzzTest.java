package net.javacrumbs.shedlock.provider.jdbctemplate;

import net.javacrumbs.shedlock.support.KeepAliveLockProvider;
import net.javacrumbs.shedlock.test.support.FuzzTester;
import net.javacrumbs.shedlock.test.support.jdbc.H2Config;
import net.javacrumbs.shedlock.test.support.jdbc.JdbcTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider.Configuration.builder;

public class KeepAliveLockProviderFuzzTest {
    private static final H2Config dbConfig = new H2Config();

    protected JdbcTestUtils testUtils;

    @BeforeEach
    public void initTestUtils() {
        testUtils = new JdbcTestUtils(dbConfig);
    }

    @AfterEach
    public void cleanup() {
        testUtils.clean();
    }

    @Test
    void keepAliveLockProviderShouldPassFuzzTest() throws ExecutionException, InterruptedException {
        JdbcTemplateLockProvider provider = new JdbcTemplateLockProvider(builder()
            .usingDbTime()
            .withJdbcTemplate(new JdbcTemplate(dbConfig.getDataSource()))
            .build());
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(20);
        KeepAliveLockProvider keepAliveLockProvider = new KeepAliveLockProvider(provider, executorService);
        new FuzzTester(keepAliveLockProvider, Duration.ofMillis(200), Duration.ofMillis(100)).doFuzzTest();
    }

    @BeforeAll
    public static void startDb() {
        dbConfig.startDb();
    }

    @AfterAll
    public static void shutDownDb() {
        dbConfig.shutdownDb();
    }
}
