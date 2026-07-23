package com.example.hotel.message.application.result;

import java.util.List;

public record JvmGcDemoStatus(
        long maxRetainedBytes,
        long retainedBytes,
        int retainedObjectCount,
        long actionCount,
        MemoryUsage heap,
        MemoryUsage nonHeap,
        List<MemoryPoolUsage> memoryPools,
        List<GarbageCollectorUsage> garbageCollectors,
        String lastAction,
        String note
) {

    public record MemoryUsage(
            long usedBytes,
            long committedBytes,
            long maxBytes,
            double usedPercent
    ) {
    }

    public record MemoryPoolUsage(
            String name,
            String type,
            long usedBytes,
            long committedBytes,
            long maxBytes,
            double usedPercent
    ) {
    }

    public record GarbageCollectorUsage(
            String name,
            long collectionCount,
            long collectionTimeMillis
    ) {
    }
}
