package com.neo4j.bench.client.profiling;

public enum RecordingType
{
    // jfr
    JFR( "jfr", ".jfr" ),
    JFR_FLAMEGRAPH( "jfr_flamegraph", "-jfr.svg" ),
    // async
    ASYNC( "async", ".async" ),
    ASYNC_FLAMEGRAPH( "async_flamegraph", "-async.svg" ),
    // gc
    GC_LOG( "gc_log", ".gc" ),
    GC_SUMMARY( "gc_summary", "-gc.json" ),
    GC_CSV( "gc_csv", "-gc.csv" ),
    // tracing
    TRACE_STRACE( "strace", "-strace.log" ),
    TRACE_MPSTAT( "mpstat", "-mpstat.log" ),
    TRACE_VMSTAT( "vmstat", "-vmstat.log" ),
    TRACE_IOSTAT( "iostat", "-iostat.log" ),
    TRACE_JVM( "jvm_log", "-jvm.log" );

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
