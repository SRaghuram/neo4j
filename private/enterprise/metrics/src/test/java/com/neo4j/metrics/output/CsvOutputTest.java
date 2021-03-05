/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.output;

import com.codahale.metrics.MetricRegistry;
import com.neo4j.configuration.MetricsSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.log4j.RotatingLogFileWriter;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.configuration.MetricsSettings.csv_path;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.fail;

@Neo4jLayoutExtension
@ExtendWith( LifeExtension.class )
class CsvOutputTest
{
    @Inject
    private LifeSupport life;
    @Inject
    private TestDirectory directory;
    @Inject
    private FileSystemAbstraction fileSystem;

    @Test
    void shouldHaveRelativeMetricsCsvPathBeRelativeToNeo4jHome() throws Exception
    {
        // GIVEN
        Path home = directory.absolutePath();
        Config config = Config.newBuilder()
                .set( MetricsSettings.csv_enabled, true )
                .set( MetricsSettings.csv_interval, Duration.ofMillis( 10 ) )
                .set( MetricsSettings.csv_path, Path.of( "the-metrics-dir" ) )
                .set( GraphDatabaseSettings.neo4j_home, home.toAbsolutePath() ).build();
        life.add( createCsvOutput( config ) );

        // WHEN
        life.start();

        // THEN
        waitForFileToAppear( home.resolve( "the-metrics-dir" ) );
    }

    @Test
    void shouldHaveAbsoluteMetricsCsvPathBeAbsolute() throws Exception
    {
        // GIVEN
        Path outputFPath = Files.createTempDirectory( "output" );
        Config config = Config.newBuilder()
                .set( MetricsSettings.csv_enabled, true )
                .set( MetricsSettings.csv_interval, Duration.ofMillis( 10 ) )
                .set( MetricsSettings.csv_path, outputFPath.toAbsolutePath() ).build();
        life.add( createCsvOutput( config ) );

        // WHEN
        life.start();

        // THEN
        waitForFileToAppear( outputFPath );
    }

    private CsvOutput createCsvOutput( Config config )
    {
        return new CsvOutput( config, NullLog.getInstance(), new RotatableCsvReporter( new MetricRegistry(), fileSystem, config.get( csv_path ),
                config.get( MetricsSettings.csv_rotation_threshold ), config.get( MetricsSettings.csv_max_archives ),
                config.get( MetricsSettings.csv_archives_compression ), RotatingLogFileWriter::new, NullLog.getInstance() ), fileSystem );
    }

    private void waitForFileToAppear( Path file ) throws InterruptedException
    {
        long end = currentTimeMillis() + SECONDS.toMillis( 10 );
        while ( Files.notExists( file ) )
        {
            Thread.sleep( 10 );
            if ( currentTimeMillis() > end )
            {
                fail( file + " didn't appear" );
            }
        }
    }
}
