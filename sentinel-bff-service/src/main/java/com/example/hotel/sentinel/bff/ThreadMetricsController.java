package com.example.hotel.sentinel.bff;

import com.example.hotel.common.api.ApiResponse;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PreDestroy;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("/lab-chain")
public class ThreadMetricsController {

    private static final int CORE_THREADS = 8;
    private static final int MAX_TASKS = 64;
    private static final long MAX_BUSY_MS = 10_000L;

    private final ThreadPoolExecutor executor;
    private final Counter requestCounter;
    private final Counter taskCounter;
    private final Counter rejectedCounter;
    private final Timer taskTimer;

    public ThreadMetricsController(MeterRegistry meterRegistry) {
        this.executor = new ThreadPoolExecutor(
                CORE_THREADS,
                CORE_THREADS,
                30L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(256),
                new LabThreadFactory());

        this.requestCounter = Counter.builder("lab_thread_test_requests")
                .description("Requests accepted by the thread metrics test endpoint")
                .register(meterRegistry);
        this.taskCounter = Counter.builder("lab_thread_test_tasks")
                .description("Tasks submitted by the thread metrics test endpoint")
                .register(meterRegistry);
        this.rejectedCounter = Counter.builder("lab_thread_test_rejected_tasks")
                .description("Tasks rejected by the thread metrics test executor")
                .register(meterRegistry);
        this.taskTimer = Timer.builder("lab_thread_test_task_duration")
                .description("Execution duration of thread metrics test tasks")
                .publishPercentileHistogram()
                .register(meterRegistry);

        Gauge.builder("lab_thread_pool_active_threads", executor, ThreadPoolExecutor::getActiveCount)
                .description("Active threads in the lab metrics executor")
                .register(meterRegistry);
        Gauge.builder("lab_thread_pool_current_threads", executor, ThreadPoolExecutor::getPoolSize)
                .description("Current threads in the lab metrics executor")
                .register(meterRegistry);
        Gauge.builder("lab_thread_pool_queue_size", executor, pool -> pool.getQueue().size())
                .description("Queued tasks in the lab metrics executor")
                .register(meterRegistry);
        Gauge.builder("lab_thread_pool_completed_tasks", executor, ThreadPoolExecutor::getCompletedTaskCount)
                .description("Completed tasks in the lab metrics executor")
                .register(meterRegistry);
    }

    @GetMapping("/thread-test")
    public ApiResponse<Map<String, Object>> threadTest(@RequestParam(name = "tasks", defaultValue = "16") Integer tasks,
                                                       @RequestParam(name = "busyMs", defaultValue = "5000") Long busyMs) {
        int taskCount = Math.max(1, Math.min(tasks == null ? 16 : tasks, MAX_TASKS));
        long durationMs = Math.max(1L, Math.min(busyMs == null ? 5000L : busyMs, MAX_BUSY_MS));
        int accepted = 0;
        int rejected = 0;

        requestCounter.increment();
        for (int i = 0; i < taskCount; i++) {
            try {
                executor.execute(() -> taskTimer.record(() -> sleep(durationMs)));
                taskCounter.increment();
                accepted++;
            } catch (RejectedExecutionException ex) {
                rejectedCounter.increment();
                rejected++;
            }
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("service", "sentinel-bff-service");
        body.put("endpoint", "/lab-chain/thread-test");
        body.put("time", Instant.now().toString());
        body.put("requestedTasks", taskCount);
        body.put("acceptedTasks", accepted);
        body.put("rejectedTasks", rejected);
        body.put("busyMs", durationMs);
        body.put("activeThreads", executor.getActiveCount());
        body.put("poolThreads", executor.getPoolSize());
        body.put("queuedTasks", executor.getQueue().size());
        body.put("completedTasks", executor.getCompletedTaskCount());
        return ApiResponse.ok(body);
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
    }

    private static void sleep(long durationMs) {
        try {
            Thread.sleep(durationMs);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
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
