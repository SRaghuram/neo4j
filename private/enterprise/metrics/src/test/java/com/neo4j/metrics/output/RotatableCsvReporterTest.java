/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.output;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Locale;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@TestDirectoryExtension
class RotatableCsvReporterTest
{
    @Inject
    private TestDirectory testDirectory;
    private final RotatingFileOutputStreamSupplier fileOutputStreamSupplier = mock( RotatingFileOutputStreamSupplier.class );

    @Test
    void stopAllWritersOnStop() throws IOException
    {
        OutputStream outputStream = mock( OutputStream.class );
        when( fileOutputStreamSupplier.get() ).thenReturn( outputStream );
        RotatableCsvReporter reporter =
                new RotatableCsvReporter( mock( MetricRegistry.class ), Locale.US, TimeUnit.SECONDS, TimeUnit.SECONDS,
                        Clock.defaultClock(), testDirectory.homePath(),
                        ( file, rotationListener ) -> fileOutputStreamSupplier );
        TreeMap<String,Gauge> gauges = new TreeMap<>();
        gauges.put( "a", () -> ThreadLocalRandom.current().nextLong() );
        gauges.put( "b", () -> ThreadLocalRandom.current().nextLong() );
        gauges.put( "c", () -> ThreadLocalRandom.current().nextLong() );
        reporter.report( gauges, new TreeMap<>(), new TreeMap<>(), new TreeMap<>(), new TreeMap<>() );

        gauges.put( "b", () -> ThreadLocalRandom.current().nextLong() );
        gauges.put( "c", () -> ThreadLocalRandom.current().nextLong() );
        gauges.put( "d", () -> ThreadLocalRandom.current().nextLong() );
        reporter.report( gauges, new TreeMap<>(), new TreeMap<>(), new TreeMap<>(), new TreeMap<>() );

        reporter.stop();
        verify( fileOutputStreamSupplier, times( 4 ) ).close();
    }
}
