/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.output;

import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiPredicate;

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
import static com.neo4j.metrics.MetricsTestHelper.readLongCounterAndAssert;
import static java.time.Duration.ofMinutes;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang3.ArrayUtils.isNotEmpty;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@TestDirectoryExtension
class RotatableCsvOutputIT
{
    @Inject
    private TestDirectory testDirectory;

    private Path outputPath;
    private GraphDatabaseService database;
    private static final BiPredicate<Long,Long> MONOTONIC = ( newValue, currentValue ) -> newValue >= currentValue;
    private static final int MAX_ARCHIVES = 20;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setup()
    {
        outputPath = testDirectory.directoryPath( "metrics" );
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() )
                .setConfig( csv_path, outputPath.toAbsolutePath() )
                .setConfig( csv_rotation_threshold, "t,count,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit".length() + 1L )
                .setConfig( csv_interval, Duration.ofMillis( 100 ) )
                .setConfig( csv_max_archives, MAX_ARCHIVES )
                .setConfig( OnlineBackupSettings.online_backup_enabled, false ).build();
        database = managementService.database( DEFAULT_DATABASE_NAME );
    }

    @AfterEach
    void tearDown()
    {
        managementService.shutdown();
    }

    @Test
    void rotateMetricsFile()
    {
        assertTimeoutPreemptively( ofMinutes( 3 ), () ->
        {
            // Commit a transaction and wait for rotation to happen
            doTransaction();
            String committedMetricFile = "neo4j.neo4j.transaction.committed.csv";

            // Latest file should now have recorded the transaction
            checkTransactionCount( committedMetricFile, 1L );

            // Commit yet another transaction and wait for it to appear in metrics
            doTransaction();

            // Latest file should now have recorded the new transaction
            checkTransactionCount( committedMetricFile, 2L );
        } );
    }

    private void checkTransactionCount( String metricFileName, long expectedValue )
    {
        assertEventually( () ->
        {
            Path metricsCsv = metricsCsv( outputPath, metricFileName );
            return readLongCounterAndAssert( metricsCsv, MONOTONIC );
        }, equalityCondition( expectedValue ), 2, TimeUnit.MINUTES );
    }

    private void doTransaction()
    {
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.createNode();
            transaction.commit();
        }
    }

    private static Path metricsCsv( Path dbDir, String metric )
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

    private static Optional<Path> findLatestMetricsFile( Path metricsPath, String metric )
    {
        String[] metricFiles = requireNonNull( metricsPath.toFile().list( ( dir, name ) -> name.equals( metric ) ) );
        if ( isNotEmpty( metricFiles ) )
        {
            return Optional.of( metricsPath.resolve( metricFiles[0] ) );
        }
        return Optional.empty();
    }

}
