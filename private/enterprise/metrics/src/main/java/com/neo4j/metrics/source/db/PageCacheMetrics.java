/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.neo4j.metrics.metric.MetricsCounter;
import com.neo4j.metrics.metric.MetricsRegister;
import com.neo4j.metrics.source.MetricGroup;
import com.neo4j.metrics.source.Metrics;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.io.pagecache.monitoring.PageCacheCounters;

import static com.codahale.metrics.MetricRegistry.name;

@ServiceProvider
@Documented( ".Database page cache metrics" )
public class PageCacheMetrics extends Metrics
{
    private static final String PAGE_CACHE_PREFIX = "page_cache";

    @Documented( "The total number of exceptions seen during the eviction process in the page cache. (counter)" )
    private static final String PC_EVICTION_EXCEPTIONS_TEMPLATE = name( PAGE_CACHE_PREFIX, "eviction_exceptions" );
    @Documented( "The total number of page flushes executed by the page cache. (counter)" )
    private static final String PC_FLUSHES_TEMPLATE = name( PAGE_CACHE_PREFIX, "flushes" );
    @Documented( "The total number of page merges executed by the page cache. (counter)" )
    private static final String PC_MERGES_TEMPLATE = name( PAGE_CACHE_PREFIX, "merges" );
    @Documented( "The total number of page unpins executed by the page cache. (counter)" )
    private static final String PC_UNPINS_TEMPLATE = name( PAGE_CACHE_PREFIX, "unpins" );
    @Documented( "The total number of page pins executed by the page cache. (counter)" )
    private static final String PC_PINS_TEMPLATE = name( PAGE_CACHE_PREFIX, "pins" );
    @Documented( "The total number of page evictions executed by the page cache. (counter)" )
    private static final String PC_EVICTIONS_TEMPLATE = name( PAGE_CACHE_PREFIX, "evictions" );
    @Documented( "The total number of page faults happened in the page cache. (counter)" )
    private static final String PC_PAGE_FAULTS_TEMPLATE = name( PAGE_CACHE_PREFIX, "page_faults" );
    @Documented( "The total number of page hits happened in the page cache. (counter)" )
    private static final String PC_HITS_TEMPLATE = name( PAGE_CACHE_PREFIX, "hits" );
    @Documented( "The ratio of hits to the total number of lookups in the page cache. (gauge)" )
    private static final String PC_HIT_RATIO_TEMPLATE = name( PAGE_CACHE_PREFIX, "hit_ratio" );
    @Documented( "The ratio of number of used pages to total number of available pages. (gauge)" )
    private static final String PC_USAGE_RATIO_TEMPLATE = name( PAGE_CACHE_PREFIX, "usage_ratio" );
    @Documented( "The total number of bytes read by the page cache. (counter)" )
    private static final String PC_BYTES_READ_TEMPLATE = name( PAGE_CACHE_PREFIX, "bytes_read" );
    @Documented( "The total number of bytes written by the page cache. (counter)" )
    private static final String PC_BYTES_WRITTEN_TEMPLATE = name( PAGE_CACHE_PREFIX, "bytes_written" );
    @Documented( "The total number of IO operations performed by page cache." )
    private static final String PC_IOPQ_TEMPLATE = name( PAGE_CACHE_PREFIX, "iops" );
    @Documented( "The total number of times page cache flush IO limiter was throttled during ongoing IO operations." )
    private static final String PC_IOPQ_LIMITER_TIMES_TEMPLATE = name( PAGE_CACHE_PREFIX, "throttled.times" );
    @Documented( "The total number of millis page cache flush IO limiter was throttled during ongoing IO operations." )
    private static final String PC_IOPQ_LIMITER_MILLIS_TEMPLATE = name( PAGE_CACHE_PREFIX, "throttled.millis" );

    private final String pcEvictionExceptions;
    private final String pcFlushes;
    private final String pcMerges;
    private final String pcUnpins;
    private final String pcPins;
    private final String pcEvictions;
    private final String pcPageFaults;
    private final String pcHits;
    private final String pcHitRatio;
    private final String pcUsageRatio;
    private final String pcBytesRead;
    private final String pcBytesWritten;
    private final String pcIOPQPerformed;
    private final String pcIOLimiterTimes;
    private final String pcIOLimiterMillis;

    private final MetricsRegister registry;
    private final PageCacheCounters pageCacheCounters;

    /**
     * Only for generating documentation. The metrics documentation is generated through
     * service loading which requires a zero-argument constructor.
     */
    public PageCacheMetrics()
    {
        this( "", null, null );
    }

    public PageCacheMetrics( String metricsPrefix, MetricsRegister registry, PageCacheCounters pageCacheCounters )
    {
        super( MetricGroup.GENERAL );
        this.registry = registry;
        this.pageCacheCounters = pageCacheCounters;
        this.pcEvictionExceptions = name( metricsPrefix, PC_EVICTION_EXCEPTIONS_TEMPLATE );
        this.pcFlushes = name( metricsPrefix, PC_FLUSHES_TEMPLATE );
        this.pcMerges = name( metricsPrefix, PC_MERGES_TEMPLATE );
        this.pcUnpins = name( metricsPrefix, PC_UNPINS_TEMPLATE );
        this.pcPins = name( metricsPrefix, PC_PINS_TEMPLATE );
        this.pcEvictions = name( metricsPrefix, PC_EVICTIONS_TEMPLATE );
        this.pcPageFaults = name( metricsPrefix, PC_PAGE_FAULTS_TEMPLATE );
        this.pcHits = name( metricsPrefix, PC_HITS_TEMPLATE );
        this.pcHitRatio = name( metricsPrefix, PC_HIT_RATIO_TEMPLATE );
        this.pcUsageRatio = name( metricsPrefix, PC_USAGE_RATIO_TEMPLATE );
        this.pcBytesRead = name( metricsPrefix, PC_BYTES_READ_TEMPLATE );
        this.pcBytesWritten = name( metricsPrefix, PC_BYTES_WRITTEN_TEMPLATE );
        this.pcIOPQPerformed = name( metricsPrefix, PC_IOPQ_TEMPLATE );
        this.pcIOLimiterTimes = name( metricsPrefix, PC_IOPQ_LIMITER_TIMES_TEMPLATE );
        this.pcIOLimiterMillis = name( metricsPrefix, PC_IOPQ_LIMITER_MILLIS_TEMPLATE );
    }

    @Override
    public void start()
    {
        registry.register( pcPageFaults, () -> new MetricsCounter( pageCacheCounters::faults ) );
        registry.register( pcEvictions, () -> new MetricsCounter( pageCacheCounters::evictions ) );
        registry.register( pcPins, () -> new MetricsCounter( pageCacheCounters::pins ) );
        registry.register( pcUnpins, () -> new MetricsCounter( pageCacheCounters::unpins ) );
        registry.register( pcHits, () -> new MetricsCounter( pageCacheCounters::hits ) );
        registry.register( pcFlushes, () -> new MetricsCounter( pageCacheCounters::flushes ) );
        registry.register( pcMerges, () -> new MetricsCounter( pageCacheCounters::merges ) );
        registry.register( pcEvictionExceptions, () -> new MetricsCounter( pageCacheCounters::evictionExceptions ) );
        registry.register( pcHitRatio, () -> new PageCacheHitRatioGauge( pageCacheCounters ) );
        registry.register( pcUsageRatio, () -> (Gauge<Double>) pageCacheCounters::usageRatio );
        registry.register( pcBytesRead, () -> new MetricsCounter( pageCacheCounters::bytesRead ) );
        registry.register( pcBytesWritten, () -> new MetricsCounter( pageCacheCounters::bytesWritten ) );
        registry.register( pcIOPQPerformed, () -> new MetricsCounter( pageCacheCounters::iopqPerformed ) );
        registry.register( pcIOLimiterTimes, () -> new MetricsCounter( pageCacheCounters::ioLimitedTimes ) );
        registry.register( pcIOLimiterMillis, () -> new MetricsCounter( pageCacheCounters::ioLimitedMillis ) );
    }

    @Override
    public void stop()
    {
        registry.remove( pcPageFaults );
        registry.remove( pcEvictions );
        registry.remove( pcPins );
        registry.remove( pcUnpins );
        registry.remove( pcHits );
        registry.remove( pcMerges );
        registry.remove( pcFlushes );
        registry.remove( pcEvictionExceptions );
        registry.remove( pcHitRatio );
        registry.remove( pcUsageRatio );
        registry.remove( pcBytesRead );
        registry.remove( pcBytesWritten );
        registry.remove( pcIOPQPerformed );
        registry.remove( pcIOLimiterTimes );
        registry.remove( pcIOLimiterMillis );
    }
}
