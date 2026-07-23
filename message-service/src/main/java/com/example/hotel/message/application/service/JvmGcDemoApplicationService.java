package com.example.hotel.message.application.service;

import com.example.hotel.message.application.result.JvmGcDemoActionResult;
import com.example.hotel.message.application.result.JvmGcDemoStatus;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class JvmGcDemoApplicationService {

    private static final int MAX_TRANSIENT_OBJECTS = 10_000;
    private static final int MAX_TRANSIENT_OBJECT_SIZE_KB = 1024;
    private static final int MAX_HUMONGOUS_OBJECTS = 100;
    private static final int MAX_HUMONGOUS_OBJECT_SIZE_MB = 16;
    private static final int MAX_EXPLICIT_GC_TIMES = 10;

    private final long maxRetainedBytes;
    private final List<byte[]> retainedObjects = new ArrayList<>();
    private final AtomicLong retainedBytes = new AtomicLong();
    private final AtomicLong actionCount = new AtomicLong();
    private final ReentrantLock lock = new ReentrantLock();
    private volatile String lastAction = "NONE";

    public JvmGcDemoApplicationService(
            MeterRegistry meterRegistry,
            @Value("${hotel.jvm-gc-demo.max-retained-bytes:134217728}") long maxRetainedBytes) {
        this.maxRetainedBytes = maxRetainedBytes;
        Gauge.builder("hotel.jvm.gc.demo.retained.bytes", retainedBytes, AtomicLong::get)
                .description("Bytes retained by the JVM GC demo endpoint")
                .baseUnit("bytes")
                .register(meterRegistry);
        Gauge.builder("hotel.jvm.gc.demo.retained.objects", retainedObjects, List::size)
                .description("Object count retained by the JVM GC demo endpoint")
                .register(meterRegistry);
        Gauge.builder("hotel.jvm.gc.demo.actions", actionCount, AtomicLong::get)
                .description("Number of JVM GC demo actions")
                .register(meterRegistry);
    }

    public JvmGcDemoActionResult allocateYoungObjects(int objects, int sizeKb) {
        int safeObjects = clamp(objects, 1, MAX_TRANSIENT_OBJECTS);
        int safeSizeKb = clamp(sizeKb, 1, MAX_TRANSIENT_OBJECT_SIZE_KB);
        long bytes = bytesFromKb(safeSizeKb) * safeObjects;
        if (bytes > maxRetainedBytes / 2) {
            return rejected("allocate-young", bytes, safeObjects,
                    "单次短命对象分配超过演示上限的一半，请降低 objects 或 sizeKb。");
        }

        List<byte[]> transientObjects = new ArrayList<>(safeObjects);
        for (int i = 0; i < safeObjects; i++) {
            byte[] payload = new byte[safeSizeKb * 1024];
            payload[0] = (byte) i;
            transientObjects.add(payload);
        }
        transientObjects.clear();

        markAction("ALLOCATE_YOUNG objects=" + safeObjects + ", sizeKb=" + safeSizeKb);
        return accepted("allocate-young", bytes, safeObjects, false,
                "已分配并释放短命对象，主要用于观察 G1 Young GC、Eden 使用率和 allocation rate。");
    }

    public JvmGcDemoActionResult retainHumongousObjects(int objects, int sizeMb) {
        int safeObjects = clamp(objects, 1, MAX_HUMONGOUS_OBJECTS);
        int safeSizeMb = clamp(sizeMb, 1, MAX_HUMONGOUS_OBJECT_SIZE_MB);
        long bytes = bytesFromMb(safeSizeMb) * safeObjects;

        lock.lock();
        try {
            long afterRetain = retainedBytes.get() + bytes;
            if (afterRetain > maxRetainedBytes) {
                return rejected("retain-humongous", bytes, safeObjects,
                        "本次保留后会超过最大保留内存 " + maxRetainedBytes
                                + " bytes。先调用 clear，或降低 objects/sizeMb。");
            }
            for (int i = 0; i < safeObjects; i++) {
                byte[] payload = new byte[safeSizeMb * 1024 * 1024];
                payload[0] = (byte) i;
                retainedObjects.add(payload);
            }
            retainedBytes.addAndGet(bytes);
        } finally {
            lock.unlock();
        }

        markAction("RETAIN_HUMONGOUS objects=" + safeObjects + ", sizeMb=" + safeSizeMb);
        return accepted("retain-humongous", bytes, safeObjects, true,
                "已保留大对象引用，用于观察 G1 humongous/old 区压力、mixed GC 和可能的 Full GC。");
    }

    public JvmGcDemoActionResult explicitGc(int times) {
        int safeTimes = clamp(times, 1, MAX_EXPLICIT_GC_TIMES);
        for (int i = 0; i < safeTimes; i++) {
            System.gc();
            sleepQuietly(100);
        }
        markAction("EXPLICIT_GC times=" + safeTimes);
        return accepted("explicit-gc", 0, 0, false,
                "已调用 System.gc()。如果 JVM 未禁用显式 GC，G1 通常会记录 System.gc() 相关的 Full GC。");
    }

    public JvmGcDemoActionResult clearRetainedObjects() {
        long clearedBytes;
        int clearedObjects;
        lock.lock();
        try {
            clearedBytes = retainedBytes.getAndSet(0);
            clearedObjects = retainedObjects.size();
            retainedObjects.clear();
        } finally {
            lock.unlock();
        }
        markAction("CLEAR retainedObjects=" + clearedObjects);
        return accepted("clear", clearedBytes, clearedObjects, false,
                "已清理演示接口保留的对象引用，下一轮 GC 后堆使用率应下降。");
    }

    public JvmGcDemoStatus status() {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        List<JvmGcDemoStatus.MemoryPoolUsage> pools = ManagementFactory.getMemoryPoolMXBeans()
                .stream()
                .map(this::toPoolUsage)
                .toList();
        List<JvmGcDemoStatus.GarbageCollectorUsage> collectors = ManagementFactory.getGarbageCollectorMXBeans()
                .stream()
                .map(this::toCollectorUsage)
                .toList();
        return new JvmGcDemoStatus(
                maxRetainedBytes,
                retainedBytes.get(),
                retainedObjects.size(),
                actionCount.get(),
                toMemoryUsage(memoryMXBean.getHeapMemoryUsage()),
                toMemoryUsage(memoryMXBean.getNonHeapMemoryUsage()),
                pools,
                collectors,
                lastAction,
                "Grafana 看趋势，GC 日志看 Full GC 原因字段，接口状态只用于快速确认当前保留对象。"
        );
    }

    private JvmGcDemoActionResult accepted(String action, long bytes, int objects, boolean retained, String message) {
        return new JvmGcDemoActionResult(action, true, bytes, objects, retained, message, status());
    }

    private JvmGcDemoActionResult rejected(String action, long bytes, int objects, String message) {
        return new JvmGcDemoActionResult(action, false, bytes, objects, false, message, status());
    }

    private void markAction(String action) {
        lastAction = Instant.now() + " " + action;
        actionCount.incrementAndGet();
    }

    private JvmGcDemoStatus.MemoryPoolUsage toPoolUsage(MemoryPoolMXBean pool) {
        MemoryUsage usage = pool.getUsage();
        return new JvmGcDemoStatus.MemoryPoolUsage(
                pool.getName(),
                pool.getType() == MemoryType.HEAP ? "heap" : "nonheap",
                usage.getUsed(),
                usage.getCommitted(),
                usage.getMax(),
                percent(usage.getUsed(), usage.getMax())
        );
    }

    private JvmGcDemoStatus.GarbageCollectorUsage toCollectorUsage(GarbageCollectorMXBean collector) {
        return new JvmGcDemoStatus.GarbageCollectorUsage(
                collector.getName(),
                collector.getCollectionCount(),
                collector.getCollectionTime()
        );
    }

    private JvmGcDemoStatus.MemoryUsage toMemoryUsage(MemoryUsage usage) {
        return new JvmGcDemoStatus.MemoryUsage(
                usage.getUsed(),
                usage.getCommitted(),
                usage.getMax(),
                percent(usage.getUsed(), usage.getMax())
        );
    }

    private double percent(long used, long max) {
        if (max <= 0) {
            return 0;
        }
        return used * 100.0 / max;
    }

    private int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }

    private long bytesFromKb(int kb) {
        return kb * 1024L;
    }

    private long bytesFromMb(int mb) {
        return mb * 1024L * 1024L;
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
