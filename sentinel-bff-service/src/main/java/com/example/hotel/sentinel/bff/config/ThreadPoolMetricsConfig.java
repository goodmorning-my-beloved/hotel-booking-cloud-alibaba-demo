package com.example.hotel.sentinel.bff.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
public class ThreadPoolMetricsConfig {

    private static final int CORE_THREADS = 8;
    private static final int QUEUE_CAPACITY = 256;
    private static final String EXECUTOR_NAME = "lab-thread-pool";

    @Bean(destroyMethod = "shutdownNow")
    public ThreadPoolExecutor labThreadPoolExecutor(MeterRegistry meterRegistry) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                CORE_THREADS,
                CORE_THREADS,
                30L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(QUEUE_CAPACITY),
                new LabThreadFactory());

        new ExecutorServiceMetrics(executor, EXECUTOR_NAME, Tags.empty()).bindTo(meterRegistry);
        return executor;
    }

    private static final class LabThreadFactory implements ThreadFactory {
        private final AtomicInteger sequence = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable);
            thread.setName("lab-metrics-worker-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }
}
