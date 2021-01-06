/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.profiling;

public enum RecordingType
{
    // jfr
    JFR( "jfr", ".jfr" ),
    JFR_FLAMEGRAPH( "jfr_flamegraph", ".jfr.svg" ),
    JFR_MEMALLOC_FLAMEGRAPH( "jfr_memalloc_flamegraph", ".jfr.memalloc.svg" ),
    // async
    ASYNC( "async", ".async" ),
    ASYNC_FLAMEGRAPH( "async_flamegraph", ".async.svg" ),
    // gc
    GC_LOG( "gc_log", ".gc" ),
    GC_SUMMARY( "gc_summary", ".gc.json" ),
    GC_CSV( "gc_csv", ".gc.csv" ),
    ASCII_PLAN( "aggregate_plan", ".ascii.plan.txt" ),
    // tracing
    TRACE_STRACE( "strace", ".strace.log" ),
    TRACE_MPSTAT( "mpstat", ".mpstat.log" ),
    TRACE_VMSTAT( "vmstat", ".vmstat.log" ),
    TRACE_IOSTAT( "iostat", ".iostat.log" ),
    TRACE_JVM( "jvm_log", ".jvm.log" ),
    NMT_SUMMARY( "nmt_summary", ".nmt.summary.csv" ),
    HEAP_DUMP( "heap_dump", ".hprof" ),
    MEMORY_ESTIMATION( "memory_estimation", "mem.json" ),
    /**
     * See {@link NoOpProfiler} for explanation about why this recording type is required.
     */
    NONE( "none", ".none" );

    private final String propertyKey;
    private final String defaultExtension;

    RecordingType( String propertyKey, String defaultExtension )
    {
        this.propertyKey = propertyKey;
        this.defaultExtension = defaultExtension;
    }

    public String propertyKey()
    {
        return propertyKey;
    }

    public String extension()
    {
        return defaultExtension;
    }
}
