/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.output;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.neo4j.configuration.MetricsSettings.CompressionOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.log4j.RotatingLogFileWriter;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.configuration.MetricsSettings.CompressionOption.NONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@TestDirectoryExtension
class RotatableCsvReporterTest
{
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fileSystemAbstraction;

    @Test
    void stopAllWritersOnStop() throws IOException
    {
        RotatingLogFileFactory factory = mock( RotatingLogFileFactory.class );
        RotatingLogFileWriter writer1 = mock( RotatingLogFileWriter.class );
        RotatingLogFileWriter writer2 = mock( RotatingLogFileWriter.class );
        RotatingLogFileWriter writer3 = mock( RotatingLogFileWriter.class );
        when( factory.createWriter( any(), any(), anyLong(), anyInt(), anyString(), anyString() ) ).thenReturn( writer1 ).thenReturn( writer2 )
                .thenReturn( writer3 );
        RotatableCsvReporter reporter =
                new RotatableCsvReporter( mock( MetricRegistry.class ), fileSystemAbstraction, testDirectory.homePath(), 10, 2, NONE, factory );
        TreeMap<String,Gauge> gauges = new TreeMap<>();
        gauges.put( "a", () -> ThreadLocalRandom.current().nextLong() );
        gauges.put( "b", () -> ThreadLocalRandom.current().nextLong() );
        reporter.report( gauges, new TreeMap<>(), new TreeMap<>(), new TreeMap<>(), new TreeMap<>() );

        gauges.put( "b", () -> ThreadLocalRandom.current().nextLong() );
        gauges.put( "c", () -> ThreadLocalRandom.current().nextLong() );
        reporter.report( gauges, new TreeMap<>(), new TreeMap<>(), new TreeMap<>(), new TreeMap<>() );

        reporter.stop();
        verify( writer1, times( 1 ) ).close();
        verify( writer2, times( 1 ) ).close();
        verify( writer3, times( 1 ) ).close();
    }

    @Test
    void newFileCreatedIfNotExists() throws IOException
    {
        String metricName = "a";
        Path csvFile = testDirectory.homePath().resolve( metricName + ".csv" );
        RotatableCsvReporter reporter =
                new RotatableCsvReporter( mock( MetricRegistry.class ), fileSystemAbstraction, testDirectory.homePath(), 10, 1, NONE,
                        RotatingLogFileWriter::new );

        TreeMap<String,Gauge> gauges = new TreeMap<>();
        gauges.put( metricName, () -> ThreadLocalRandom.current().nextLong() );
        reporter.report( gauges, new TreeMap<>(), new TreeMap<>(), new TreeMap<>(), new TreeMap<>() );

        assertTrue( Files.exists( csvFile ) );
        assertThat( Files.readAllLines( csvFile ) ).hasSize( 2 ); // header line and one with metrics
        reporter.stop();
    }

    @ParameterizedTest
    @EnumSource( CompressionOption.class )
    void compressionCorrectlyConvertedToFileSuffix( CompressionOption compression )
    {
        String metricName = "a";
        Path csvFile = testDirectory.homePath().resolve( metricName + ".csv" );
        RotatingLogFileFactory factory = mock( RotatingLogFileFactory.class );
        when( factory.createWriter( any(), any(), anyLong(), anyInt(), anyString(), anyString() ) ).thenReturn( mock( RotatingLogFileWriter.class ) );
        RotatableCsvReporter reporter =
                new RotatableCsvReporter( mock( MetricRegistry.class ), fileSystemAbstraction, testDirectory.homePath(), 10, 1, compression, factory );

        TreeMap<String,Gauge> gauges = new TreeMap<>();
        gauges.put( metricName, () -> ThreadLocalRandom.current().nextLong() );
        reporter.report( gauges, new TreeMap<>(), new TreeMap<>(), new TreeMap<>(), new TreeMap<>() );

        String expectedFileSuffix;
        switch ( compression )
        {
        case GZ:
        case ZIP:
            expectedFileSuffix = "." + compression.name().toLowerCase();
            break;
        case NONE:
            expectedFileSuffix = "";
            break;
        default:
            throw new IllegalArgumentException(
                    "new compression type seems to have been added, make sure that it is supported by underlying logging framework" );
        }
        verify( factory ).createWriter( fileSystemAbstraction, csvFile, 10, 1, expectedFileSuffix, "t,value%n" );
        reporter.stop();
    }
}
