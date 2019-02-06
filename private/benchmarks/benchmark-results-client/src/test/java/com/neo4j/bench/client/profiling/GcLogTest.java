/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.profiling;

import com.neo4j.bench.client.profiling.GcLog.EventType;
import com.neo4j.bench.client.profiling.GcLog.GcLogEvent;
import com.neo4j.bench.client.util.JsonUtil;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;

public class GcLogTest
{
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void shouldSerializeGcLogEvent() throws IOException
    {
        LocalDateTime wallClockTime = LocalDateTime.now();
        Duration processTime = Duration.ofMillis( 1 );
        GcLog.EventType eventTime = GcLog.EventType.APPLICATION_TIME;
        Duration value = Duration.ofNanos( 40 );
        Duration cumulativeValue = Duration.ofSeconds( 3 );
        GcLog.GcLogEvent gcLogEvent = new GcLog.GcLogEvent( wallClockTime, processTime, eventTime, value, cumulativeValue );

        assertThat( gcLogEvent.wallClockTime(), equalTo( wallClockTime ) );
        assertThat( gcLogEvent.processTime(), equalTo( processTime ) );
        assertThat( gcLogEvent.eventType(), equalTo( eventTime ) );
        assertThat( gcLogEvent.value(), equalTo( value ) );
        assertThat( gcLogEvent.cumulativeValue(), equalTo( cumulativeValue ) );

        File gcLogEventJson = temporaryFolder.newFile();
        JsonUtil.serializeJson( gcLogEventJson.toPath(), gcLogEvent );
        GcLog.GcLogEvent deserializeGcLogEvent = JsonUtil.deserializeJson( gcLogEventJson.toPath(), GcLog.GcLogEvent.class );
        assertThat( deserializeGcLogEvent, equalTo( gcLogEvent ) );
    }

    @Test
    public void writeResultsToCsv() throws IOException
    {
        File gcLogFile = FileUtils.toFile( GcLogTest.class.getResource( "/gc-jdk8.log" ) );
        GcLog gcLog = GcLog.parse( gcLogFile.toPath() );

        Path csv = temporaryFolder.newFile().toPath();
        gcLog.toCSV( csv );
        long actualCsvRowCount = Files.lines( csv ).count();
        long expectCsvRowCount = gcLog.events().size() + 1;
        assertThat( actualCsvRowCount, equalTo( expectCsvRowCount ) );
        gcLog.toCSV( csv );
        actualCsvRowCount = Files.lines( csv ).count();
        assertThat( actualCsvRowCount, equalTo( expectCsvRowCount ) );

    }

    @Test
    public void writeResultsToJson() throws IOException
    {
        File gcLogFile = FileUtils.toFile( GcLogTest.class.getResource( "/gc-jdk8.log" ) );
        GcLog gcLog = GcLog.parse( gcLogFile.toPath() );

        File gcLogJson = temporaryFolder.newFile();
        JsonUtil.serializeJson( gcLogJson.toPath(), gcLog );

        GcLog deserializeGcLog = JsonUtil.deserializeJson( gcLogJson.toPath(), GcLog.class );
        assertThat( deserializeGcLog, equalTo( gcLog ) );

    }

    @Test
    public void parseJdk8GcLogs() throws IOException
    {

        // given
        Path gcLogFile = FileUtils.toFile( GcLogTest.class.getResource( "/gc-jdk8.log" ) ).toPath();

        // when
        GcLog gcLog = GcLog.parse( gcLogFile );

        // then
        assertEquals( 128, gcLog.countFor( com.neo4j.bench.client.profiling.GcLog.EventType.APPLICATION_STOPPED_TIME ) );
        Duration totalFor = gcLog.totalFor( com.neo4j.bench.client.profiling.GcLog.EventType.APPLICATION_STOPPED_TIME );
        GcLogEvent lastGcEvent = gcLog.events().stream().filter( e -> e.eventType().equals( EventType.APPLICATION_STOPPED_TIME ) ).reduce( ( f,s ) -> s ).get();
        assertEquals( Duration.ofMillis( 480), totalFor );
        assertEquals( lastGcEvent.cumulativeValue().toMillis(), totalFor.toMillis() );
        assertEquals( 0.10634295697944555, gcLog.percentageFor( com.neo4j.bench.client.profiling.GcLog.EventType.APPLICATION_STOPPED_TIME ) ,0 );

        assertEquals( 141, gcLog.countFor( com.neo4j.bench.client.profiling.GcLog.EventType.GC_PAUSE ) );
        assertEquals( Duration.ofMillis( 4041), gcLog.totalFor( com.neo4j.bench.client.profiling.GcLog.EventType.GC_PAUSE) );
        assertEquals( 0.8936570430205545, gcLog.percentageFor( com.neo4j.bench.client.profiling.GcLog.EventType.GC_PAUSE ) ,0 );
    }

    @Test
    public void parseInvalidJdk8GcLogs() throws IOException
    {

        // given
        Path gcLogFile = FileUtils.toFile( GcLogTest.class.getResource( "/invalid-gc-jdk8.log" ) ).toPath();

        // when
        GcLog gcLog = GcLog.parse( gcLogFile );

        // then
        assertEquals( 1, gcLog.unparsableEvents() );

        assertEquals( 877, gcLog.countFor( com.neo4j.bench.client.profiling.GcLog.EventType.APPLICATION_STOPPED_TIME ) );
        Duration totalFor = gcLog.totalFor( com.neo4j.bench.client.profiling.GcLog.EventType.APPLICATION_STOPPED_TIME );
        GcLogEvent lastGcEvent = gcLog.events().stream().filter( e -> e.eventType().equals( EventType.APPLICATION_STOPPED_TIME ) ).reduce( ( f,s ) -> s ).get();
        assertEquals( Duration.ofMillis( 993), totalFor );
        assertEquals( lastGcEvent.cumulativeValue().toMillis(), totalFor.toMillis() );
        assertEquals( 0.025534611766541544, gcLog.percentageFor( com.neo4j.bench.client.profiling.GcLog.EventType.APPLICATION_STOPPED_TIME ) ,0 );

        assertEquals( 517, gcLog.countFor( com.neo4j.bench.client.profiling.GcLog.EventType.GC_PAUSE ) );
        assertEquals( Duration.ofMillis( 30408), gcLog.totalFor( com.neo4j.bench.client.profiling.GcLog.EventType.GC_PAUSE) );
        assertEquals( 0.7813041113881746, gcLog.percentageFor( com.neo4j.bench.client.profiling.GcLog.EventType.GC_PAUSE ) ,0 );
    }

    @Test
    public void parseJdk10GcLogs() throws IOException
    {

        // given
        Path gcLogFile = FileUtils.toFile( GcLogTest.class.getResource( "/gc-jdk10.log" ) ).toPath();

        // when
        GcLog gcLog = GcLog.parse( gcLogFile );

        // then
        // commented as gcviewer doesn't support application stopped time as of now
        //        assertEquals( 128, parse.countFor( com.neo4j.bench.client.profiling.GcLog.EventType.APPLICATION_STOPPED_TIME ) );
        //        assertEquals( Duration.ofMillis( 480), parse.totalFor( com.neo4j.bench.client.profiling.GcLog.EventType.APPLICATION_STOPPED_TIME ) );
        //        assertEquals( 0.10634295697944555, parse.percentageFor( com.neo4j.bench.client.profiling.GcLog.EventType.APPLICATION_STOPPED_TIME ) ,0 );

        assertEquals( 35, gcLog.countFor( com.neo4j.bench.client.profiling.GcLog.EventType.GC_PAUSE ) );
        assertEquals( Duration.ofMillis( 1133), gcLog.totalFor( com.neo4j.bench.client.profiling.GcLog.EventType.GC_PAUSE) );
        assertEquals( 1.0, gcLog.percentageFor( com.neo4j.bench.client.profiling.GcLog.EventType.GC_PAUSE ) ,0 );
    }
}
