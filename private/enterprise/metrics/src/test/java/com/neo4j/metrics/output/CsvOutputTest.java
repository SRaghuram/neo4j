/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.output;

import com.codahale.metrics.MetricRegistry;
import com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.extension.context.DatabaseExtensionContext;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLog;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.fail;

@TestDirectoryExtension
@ExtendWith( LifeExtension.class )
class CsvOutputTest
{
    @Inject
    private LifeSupport life;
    @Inject
    private TestDirectory directory;
    @Inject
    private FileSystemAbstraction fileSystem;
    private final JobScheduler jobScheduler = new OnDemandJobScheduler();

    private ExtensionContext extensionContext;

    @BeforeEach
    void setup()
    {
        extensionContext = new DatabaseExtensionContext( directory.databaseLayout(), DatabaseInfo.UNKNOWN, new Dependencies() );
    }

    @Test
    void shouldHaveRelativeMetricsCsvPathBeRelativeToNeo4jHome() throws Exception
    {
        // GIVEN
        File home = directory.absolutePath();
        Config config = Config.newBuilder()
                .set( MetricsSettings.csvEnabled, true )
                .set( MetricsSettings.csvInterval, Duration.ofMillis( 10 ) )
                .set( MetricsSettings.csvPath, Path.of( "the-metrics-dir" ) )
                .set( GraphDatabaseSettings.neo4j_home, home.toPath().toAbsolutePath() ).build();
        life.add( createCsvOutput( config ) );

        // WHEN
        life.start();

        // THEN
        waitForFileToAppear( new File( home, "the-metrics-dir" ) );
    }

    @Test
    void shouldHaveAbsoluteMetricsCsvPathBeAbsolute() throws Exception
    {
        // GIVEN
        File outputFPath = Files.createTempDirectory( "output" ).toFile();
        Config config = Config.newBuilder()
                .set( MetricsSettings.csvEnabled, true )
                .set( MetricsSettings.csvInterval, Duration.ofMillis( 10 ) )
                .set( MetricsSettings.csvPath, outputFPath.toPath().toAbsolutePath() ).build();
        life.add( createCsvOutput( config ) );

        // WHEN
        life.start();

        // THEN
        waitForFileToAppear( outputFPath );
    }

    private CsvOutput createCsvOutput( Config config )
    {
        return new CsvOutput( config, new MetricRegistry(), NullLog.getInstance(), extensionContext, fileSystem, jobScheduler );
    }

    private void waitForFileToAppear( File file ) throws InterruptedException
    {
        long end = currentTimeMillis() + SECONDS.toMillis( 10 );
        while ( !file.exists() )
        {
            Thread.sleep( 10 );
            if ( currentTimeMillis() > end )
            {
                fail( file + " didn't appear" );
            }
        }
    }
}
