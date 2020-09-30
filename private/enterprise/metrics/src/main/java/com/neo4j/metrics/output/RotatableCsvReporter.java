/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.output;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.neo4j.configuration.MetricsSettings;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.log4j.RotatingLogFileWriter;

import static com.neo4j.configuration.MetricsSettings.CompressionOption.NONE;

public class RotatableCsvReporter extends ScheduledReporter
{
    private final Path directory;
    private final Map<Path,RotatingLogFileWriter> writers;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final long rotationThreshold;
    private final int maxArchives;
    private final MetricsSettings.CompressionOption compression;
    private final RotatingLogFileFactory logFileFactory;

    private boolean stopped;

    RotatableCsvReporter( MetricRegistry registry, FileSystemAbstraction fileSystemAbstraction, Path directory, long rotationThreshold, int maxArchives,
            MetricsSettings.CompressionOption compression, RotatingLogFileFactory logFileFactory )
    {
        super( registry, "csv-reporter", MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS );
        this.directory = directory;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.rotationThreshold = rotationThreshold;
        this.maxArchives = maxArchives;
        this.compression = compression;
        this.logFileFactory = logFileFactory;
        this.writers = new HashMap<>();
    }

    @Override
    public synchronized void stop()
    {
        super.stop();
        stopped = true;
        IOUtils.closeAllSilently( writers.values() );
    }

    @Override
    public synchronized void report( SortedMap<String,Gauge> gauges, SortedMap<String,Counter> counters,
            SortedMap<String,Histogram> histograms, SortedMap<String,Meter> meters, SortedMap<String,Timer> timers )
    {
        if ( stopped )
        {
            return;
        }
        final long timestamp = TimeUnit.MILLISECONDS.toSeconds( Clock.defaultClock().getTime() );

        for ( Map.Entry<String,Gauge> entry : gauges.entrySet() )
        {
            reportGauge( timestamp, entry.getKey(), entry.getValue() );
        }

        for ( Map.Entry<String,Counter> entry : counters.entrySet() )
        {
            reportCounter( timestamp, entry.getKey(), entry.getValue() );
        }

        for ( Map.Entry<String,Histogram> entry : histograms.entrySet() )
        {
            reportHistogram( timestamp, entry.getKey(), entry.getValue() );
        }

        for ( Map.Entry<String,Meter> entry : meters.entrySet() )
        {
            reportMeter( timestamp, entry.getKey(), entry.getValue() );
        }

        for ( Map.Entry<String,Timer> entry : timers.entrySet() )
        {
            reportTimer( timestamp, entry.getKey(), entry.getValue() );
        }
    }

    private void reportTimer( long timestamp, String name, Timer timer )
    {
        final Snapshot snapshot = timer.getSnapshot();

        report( timestamp, name,
                "count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit,duration_unit",
                "%d,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,calls/%s,%s", timer.getCount(),
                convertDuration( snapshot.getMax() ), convertDuration( snapshot.getMean() ),
                convertDuration( snapshot.getMin() ), convertDuration( snapshot.getStdDev() ),
                convertDuration( snapshot.getMedian() ), convertDuration( snapshot.get75thPercentile() ),
                convertDuration( snapshot.get95thPercentile() ), convertDuration( snapshot.get98thPercentile() ),
                convertDuration( snapshot.get99thPercentile() ), convertDuration( snapshot.get999thPercentile() ),
                convertRate( timer.getMeanRate() ), convertRate( timer.getOneMinuteRate() ),
                convertRate( timer.getFiveMinuteRate() ), convertRate( timer.getFifteenMinuteRate() ), getRateUnit(),
                getDurationUnit() );
    }

    private void reportMeter( long timestamp, String name, Meter meter )
    {
        report( timestamp, name, "count,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit", "%d,%f,%f,%f,%f,events/%s",
                meter.getCount(), convertRate( meter.getMeanRate() ), convertRate( meter.getOneMinuteRate() ),
                convertRate( meter.getFiveMinuteRate() ), convertRate( meter.getFifteenMinuteRate() ), getRateUnit() );
    }

    private void reportHistogram( long timestamp, String name, Histogram histogram )
    {
        final Snapshot snapshot = histogram.getSnapshot();

        report( timestamp, name, "count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999",
                "%d,%d,%f,%d,%f,%f,%f,%f,%f,%f,%f", histogram.getCount(), snapshot.getMax(), snapshot.getMean(),
                snapshot.getMin(), snapshot.getStdDev(), snapshot.getMedian(), snapshot.get75thPercentile(),
                snapshot.get95thPercentile(), snapshot.get98thPercentile(), snapshot.get99thPercentile(),
                snapshot.get999thPercentile() );
    }

    private void reportCounter( long timestamp, String name, Counter counter )
    {
        report( timestamp, name, "count", "%d", counter.getCount() );
    }

    private void reportGauge( long timestamp, String name, Gauge gauge )
    {
        report( timestamp, name, "value", "%s", gauge.getValue() );
    }

    private void report( long timestamp, String name, String header, String line, Object... values )
    {
        Path file = directory.resolve( name + ".csv" );

        RotatingLogFileWriter csvWriter = writers.computeIfAbsent( file,
                fileName -> logFileFactory.createWriter( fileSystemAbstraction, fileName, rotationThreshold, maxArchives,
                        compression == NONE ? "" : "." + compression.name().toLowerCase(), "t," + header + "%n" ) );
        csvWriter.printf( String.format( Locale.US, "%d,%s", timestamp, line ), values );
    }
}
