/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.common.util.BenchmarkUtil;
import com.tagtraum.perf.gcviewer.imp.DataReader;
import com.tagtraum.perf.gcviewer.imp.DataReaderFactory;
import com.tagtraum.perf.gcviewer.model.AbstractGCEvent;
import com.tagtraum.perf.gcviewer.model.GCModel;
import com.tagtraum.perf.gcviewer.model.GCResource;
import com.tagtraum.perf.gcviewer.model.GcResourceFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.neo4j.bench.common.profiling.GcLog.EventType.APPLICATION_STOPPED_TIME;
import static com.neo4j.bench.common.profiling.GcLog.EventType.CONCURRENT_GC_PHASE;
import static com.neo4j.bench.common.profiling.GcLog.EventType.GC_PAUSE;

public class GcLog
{
    private static final Logger LOG = LoggerFactory.getLogger( GcLog.class );

    enum EventType
    {
        // application time is runtime minus application stopped time (which is GC_PAUSE + others)
        APPLICATION_TIME,
        GC_PAUSE,
        CONCURRENT_GC_PHASE,
        APPLICATION_STOPPED_TIME
    }

    private final List<GcLogEvent> events = new ArrayList<>();
    private final Map<EventType,Long> counts = new HashMap<>();
    private final Map<EventType,Long> totalMillis = new HashMap<>();
    private final Map<EventType,Double> percentages = new HashMap<>();
    private int unparsableEvents;

    public static GcLog parse( Path gcLogFile ) throws IOException
    {
        GCModel gcModel = readGcModel( gcLogFile );
        GcLog gcLog = parseGCEvents( gcModel );
        calculateTotals( gcLog );
        return gcLog;
    }

    private static GcLog parseGCEvents( GCModel gcModel )
    {
        GcLog gcLog = new GcLog();
        Iterator<AbstractGCEvent<?>> iterator = gcModel.getEvents();
        Map<EventType,Duration> cumulative = new HashMap<>();
        while ( iterator.hasNext() )
        {
            AbstractGCEvent<?> gcEvent = iterator.next();
            gcLog.parse( gcEvent, cumulative );
        }
        return gcLog;
    }

    private static void calculateTotals( GcLog gcLog )
    {
        Arrays.stream( EventType.values() )
              .forEach( eventType -> gcLog.counts.put( eventType, gcLog.calculateCountFor( eventType ) ) );
        Arrays.stream( EventType.values() ).forEach(
                eventType -> gcLog.totalMillis.put( eventType, gcLog.calculateTotalFor( eventType ).toMillis() ) );
        Arrays.stream( EventType.values() )
              .forEach( eventType -> gcLog.percentages.put( eventType, gcLog.calculatePercentageFor( eventType ) ) );
    }

    private static GCModel readGcModel( Path gcLogFile ) throws IOException
    {
        DataReaderFactory drf = new DataReaderFactory();
        GCResource gcResource = new GcResourceFile( gcLogFile.toFile() );
        InputStream inputStream = new FileInputStream( gcLogFile.toFile() );
        DataReader dataReader = drf.getDataReader( gcResource, inputStream );
        GCModel gcModel = dataReader.read();
        return gcModel;
    }

    public void toCSV( Path csv ) throws IOException
    {
        BenchmarkUtil.forceRecreateFile( csv );
        try ( BufferedWriter writer = Files.newBufferedWriter( csv ) )
        {
            writer.write( "date_time_utc,process_time_ns,event_type,value_ns,cumulative_value_ns\n" );
            for ( GcLogEvent e : events )
            {
                writer.write( e.wallClockTimeMilli + "," + e.processTimeNano + "," + e.eventType + "," + e.valueNano + "," + e.cumulativeValueNano + "\n" );
            }
        }
    }

    public Duration totalFor( EventType eventType )
    {
        return Duration.ofMillis( totalMillis.get( eventType ) );
    }

    public long countFor( EventType eventType )
    {
        return counts.get( eventType );
    }

    public double percentageFor( EventType eventType )
    {
        return percentages.get( eventType );
    }

    public List<GcLogEvent> events()
    {
        return events;
    }

    public int unparsableEvents()
    {
        return unparsableEvents;
    }

    private Duration calculateTotalFor( EventType eventType )
    {
        return events.stream()
                     .filter( event -> event.eventType.equals( eventType ) )
                     .map( GcLogEvent::value )
                     .reduce( Duration.ZERO, Duration::plus );
    }

    private long calculateCountFor( EventType eventType )
    {
        return events.stream().filter( event -> event.eventType.equals( eventType ) ).count();
    }

    private double calculatePercentageFor( EventType eventType )
    {
        Duration total = Arrays.stream( EventType.values() )
                               .map( this::calculateTotalFor )
                               .reduce( Duration.ZERO, Duration::plus );
        Duration totalForEvent = calculateTotalFor( eventType );
        return (double) totalForEvent.toNanos() / total.toNanos();
    }

    private void parse( AbstractGCEvent<?> gcEvent, Map<EventType,Duration> cumulative )
    {
        if ( isApplicationStopped( gcEvent ) )
        {
            addGcEvent( gcEvent, cumulative, APPLICATION_STOPPED_TIME );
        }
        else if ( !gcEvent.isConcurrent() )
        {
            addGcEvent( gcEvent, cumulative, GC_PAUSE );
        }
        else if ( gcEvent.isConcurrent() )
        {
            addGcEvent( gcEvent, cumulative, CONCURRENT_GC_PHASE );
        }
    }

    private void addGcEvent( AbstractGCEvent<?> gcEvent, Map<EventType,Duration> cumulative, EventType type )
    {
        if ( gcEvent.getDatestamp() == null )
        {
            // ignore unparsable GC event,
            // this may happend when
            // IO operations (logging) interleave
            LOG.debug( "Failed to parse event: " + gcEvent.toString() );
            unparsableEvents++;
            return;
        }

        LocalDateTime wallClockTime = gcEvent.getDatestamp().toLocalDateTime();
        Duration processTime = Duration.ofNanos( Math.round( gcEvent.getTimestamp() * 1000_000_000 ) );
        Duration pause = Duration.ofNanos( Math.round( gcEvent.getPause() * 1000_000_000 ) );
        Duration cumulativeApplicationTime = cumulative.compute( type, ( eventType, current ) -> null == current ? pause : current.plus( pause ) );
        events.add( new GcLogEvent( wallClockTime, processTime, type, pause, cumulativeApplicationTime ) );
    }

    private static boolean isApplicationStopped( AbstractGCEvent<?> gcEvent )
    {
        return gcEvent.getExtendedType().getType().equals( AbstractGCEvent.Type.APPLICATION_STOPPED_TIME );
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        int totalEventCount = events.size();
        Duration totalDuration = Duration.ofNanos(
                events.stream().map( GcLogEvent::processTime ).mapToLong( Duration::toNanos ).max().getAsLong() );
        sb.append( "=========================================\n" );
        sb.append( "Total event count: " + totalEventCount + "\n" );
        sb.append( "Total duration   : " + totalDuration + "\n" );
        sb.append( "=========================================\n" );
        sb.append( "Event counts:\n" );
        Arrays.stream( EventType.values() )
              .forEach( eventType -> sb.append( "\t* " + eventType + " = " + countFor( eventType ) + "\n" ) );
        sb.append( "Event totals:\n" );
        Arrays.stream( EventType.values() )
              .forEach( eventType -> sb.append( "\t* " + eventType + " = " + totalFor( eventType ) + "\n" ) );
        sb.append( "Event percentages:\n" );
        DecimalFormat format = new DecimalFormat( "##0.00" );
        Arrays.stream( EventType.values() ).forEach( eventType -> sb
                .append( "\t* " + eventType + " = " + format.format( percentageFor( eventType ) * 100 ) + " %\n" ) );
        sb.append( "=========================================\n" );
        return sb.toString();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        GcLog gcLog = (GcLog) o;
        return Objects.equals( events, gcLog.events ) &&
               Objects.equals( counts, gcLog.counts ) &&
               Objects.equals( totalMillis, gcLog.totalMillis ) &&
               Objects.equals( percentages, gcLog.percentages );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( events, counts, totalMillis, percentages );
    }

    public static class GcLogEvent
    {
        private static final ZoneId UTC = ZoneId.of( "UTC" );
        private final long wallClockTimeMilli;
        private final long processTimeNano;
        private final EventType eventType;
        private long valueNano;
        private long cumulativeValueNano;

        /**
         * WARNING: Never call this explicitly.
         * No-params constructor is only used for JSON (de)serialization.
         */
        public GcLogEvent()
        {
            this( LocalDateTime.now(), Duration.ZERO, EventType.APPLICATION_TIME, Duration.ZERO, Duration.ZERO );
        }

        GcLogEvent( LocalDateTime wallClockTime, Duration processTime, EventType eventType, Duration value,
                    Duration cumulativeValue )
        {
            this.wallClockTimeMilli = Date.from( wallClockTime.atZone( UTC ).toInstant() ).getTime();
            this.processTimeNano = processTime.toNanos();
            this.eventType = eventType;
            this.valueNano = (null == value) ? 0 : value.toNanos();
            this.cumulativeValueNano = (null == value) ? 0 : cumulativeValue.toNanos();
        }

        public LocalDateTime wallClockTime()
        {
            return LocalDateTime.ofInstant( Instant.ofEpochMilli( wallClockTimeMilli ), UTC );
        }

        public Duration processTime()
        {
            return Duration.ofNanos( processTimeNano );
        }

        public EventType eventType()
        {
            return eventType;
        }

        public Duration value()
        {
            return Duration.ofNanos( valueNano );
        }

        public Duration cumulativeValue()
        {
            return Duration.ofNanos( cumulativeValueNano );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            GcLogEvent that = (GcLogEvent) o;
            return wallClockTimeMilli == that.wallClockTimeMilli &&
                   processTimeNano == that.processTimeNano &&
                   valueNano == that.valueNano &&
                   cumulativeValueNano == that.cumulativeValueNano &&
                   eventType == that.eventType;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( wallClockTimeMilli, processTimeNano, eventType, valueNano, cumulativeValueNano );
        }

        @Override
        public String toString()
        {
            return "(" + wallClockTime() + " , " + processTime() + " , " + eventType + " , " + value() + " , " + cumulativeValue() + ")";
        }
    }
}
