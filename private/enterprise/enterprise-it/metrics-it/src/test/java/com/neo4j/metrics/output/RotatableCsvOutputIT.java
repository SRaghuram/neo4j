/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.output;

import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiPredicate;

import org.neo4j.configuration.helpers.GlobbingPattern;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.configuration.MetricsSettings.csv_interval;
import static com.neo4j.configuration.MetricsSettings.csv_max_archives;
import static com.neo4j.configuration.MetricsSettings.csv_path;
import static com.neo4j.configuration.MetricsSettings.csv_rotation_threshold;
import static com.neo4j.configuration.MetricsSettings.metrics_filter;
import static com.neo4j.configuration.MetricsSettings.metrics_namespaces_enabled;
import static com.neo4j.metrics.MetricsTestHelper.readLongCounterAndAssert;
import static java.time.Duration.ofMinutes;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;
import static org.neo4j.test.conditions.Conditions.greaterThan;

@TestDirectoryExtension
class RotatableCsvOutputIT
{
    @Inject
    private TestDirectory testDirectory;

    private Path outputPath;
    private static final BiPredicate<Long,Long> MONOTONIC = ( newValue, currentValue ) -> newValue >= currentValue;
    private static final int MAX_ARCHIVES = 20;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setup()
    {
        outputPath = testDirectory.directory( "metrics" );
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() )
                .setConfig( csv_path, outputPath.toAbsolutePath() )
                .setConfig( csv_rotation_threshold, "t,count,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit".length() + 1L )
                .setConfig( csv_interval, Duration.ofMillis( 100 ) )
                .setConfig( csv_max_archives, MAX_ARCHIVES )
                .setConfig( metrics_namespaces_enabled, true )
                .setConfig( OnlineBackupSettings.online_backup_enabled, false )
                .setConfig( metrics_filter, GlobbingPattern.create( "*" ) ).build();
    }

    @AfterEach
    void tearDown()
    {
        managementService.shutdown();
    }

    @Test
    void rotateMetricsFile()
    {
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );

        assertTimeoutPreemptively( ofMinutes( 3 ), () ->
        {
            // Commit a transaction and wait for rotation to happen
            doTransaction( database );
            String committedMetricFile = "neo4j.database.neo4j.transaction.committed.csv";

            // Latest file should now have recorded the transaction
            checkTransactionCount( committedMetricFile, 1L );

            // Commit yet another transaction and wait for it to appear in metrics
            doTransaction( database );

            // Latest file should now have recorded the new transaction
            checkTransactionCount( committedMetricFile, 2L );
        } );
    }

    @Test
    void droppingDatabaseShouldRemoveMetricFiles()
    {
        String databaseName = "dropper";
        String committedMetricFile = "neo4j.database.dropper.transaction.committed.csv";
        managementService.createDatabase( databaseName );
        GraphDatabaseService database = managementService.database( databaseName );

        // Ensue we do have some metrics
        doTransaction( database );
        checkTransactionCount( committedMetricFile, 1L );
        assertEventually( () -> getNumberOfFiles( outputPath, "neo4j.database.dropper.*" ), greaterThan( 0 ), 2, TimeUnit.MINUTES );

        // Drop database should removes the files belonging to that database ...
        managementService.dropDatabase( databaseName );
        assertEventually( () -> getNumberOfFiles( outputPath, "neo4j.database.dropper.*" ), equalityCondition( 0 ), 2, TimeUnit.MINUTES );

        // .. but not all of the metrics files
        assertEventually( () -> getNumberOfFiles( outputPath, "*" ), greaterThan( 0 ), 2, TimeUnit.MINUTES );
    }

    private static int getNumberOfFiles( Path outputPath, String glob ) throws IOException
    {
        int numberOfFiles = 0;
        try ( DirectoryStream<Path> paths = Files.newDirectoryStream( outputPath, glob ) )
        {
            for ( Path path : paths )
            {
                numberOfFiles++;
            }
        }
        return numberOfFiles;
    }

    private void checkTransactionCount( String metricFileName, long expectedValue )
    {
        assertEventually( () ->
        {
            Path metricsCsv = metricsCsv( outputPath, metricFileName );
            return readLongCounterAndAssert( metricsCsv, MONOTONIC );
        }, equalityCondition( expectedValue ), 2, TimeUnit.MINUTES );
    }

    private static void doTransaction( GraphDatabaseService database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.createNode();
            transaction.commit();
        }
    }

    private static Path metricsCsv( Path dbDir, String metric ) throws IOException
    {
        while ( true )
        {
            Optional<Path> metricsFile = findLatestMetricsFile( dbDir, metric );
            if ( metricsFile.isPresent() )
            {
                return metricsFile.get();
            }
            LockSupport.parkNanos( MILLISECONDS.toNanos( 10 ) );
        }
    }

    private static Optional<Path> findLatestMetricsFile( Path metricsPath, String metric ) throws IOException
    {
        try ( DirectoryStream<Path> paths = Files.newDirectoryStream( metricsPath, p -> p.getFileName().toString().equals( metric ) ) )
        {
            for ( Path path : paths )
            {
                return Optional.of( path );
            }
        }
        return Optional.empty();
    }

}
