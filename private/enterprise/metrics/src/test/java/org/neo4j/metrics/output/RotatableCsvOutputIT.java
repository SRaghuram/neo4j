/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.output;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiPredicate;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.time.Duration.ofMinutes;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang3.ArrayUtils.isNotEmpty;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.neo4j.metrics.MetricsSettings.csvMaxArchives;
import static org.neo4j.metrics.MetricsSettings.csvPath;
import static org.neo4j.metrics.MetricsSettings.csvRotationThreshold;
import static org.neo4j.metrics.MetricsTestHelper.readLongCounterAndAssert;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ExtendWith( TestDirectoryExtension.class )
class RotatableCsvOutputIT
{
    @Inject
    private TestDirectory testDirectory;

    private File outputPath;
    private GraphDatabaseService database;
    private static final BiPredicate<Long,Long> MONOTONIC = ( newValue, currentValue ) -> newValue >= currentValue;
    private static final int MAX_ARCHIVES = 20;

    @BeforeEach
    void setup()
    {
        outputPath = testDirectory.directory( "metrics" );
        database = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDirectory.storeDir() )
                .setConfig( csvPath, outputPath.getAbsolutePath() )
                .setConfig( csvRotationThreshold, "21" )
                .setConfig( csvMaxArchives, String.valueOf( MAX_ARCHIVES ) )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();
    }

    @AfterEach
    void tearDown()
    {
        database.shutdown();
    }

    @Test
    void rotateMetricsFile()
    {
        assertTimeoutPreemptively( ofMinutes( 3 ), () ->
        {
            // Commit a transaction and wait for rotation to happen
            doTransaction();
            String committedMetricFile = "neo4j.transaction.committed.csv";

            // Latest file should now have recorded the transaction
            checkTransactionCount( committedMetricFile, 1L );

            // Commit yet another transaction and wait for it to appear in metrics
            doTransaction();

            // Latest file should now have recorded the new transaction
            checkTransactionCount( committedMetricFile, 2L );
        } );
    }

    private void checkTransactionCount( String metricFileName, long expectedValue ) throws Exception
    {
        assertEventually( () ->
        {
            File metricsCsv = metricsCsv( outputPath, metricFileName );
            return readLongCounterAndAssert( metricsCsv, MONOTONIC );
        }, equalTo( expectedValue ), 2, TimeUnit.MINUTES );
    }

    private void doTransaction()
    {
        try ( Transaction transaction = database.beginTx() )
        {
            database.createNode();
            transaction.success();
        }
    }

    private static File metricsCsv( File dbDir, String metric )
    {
        while ( true )
        {
            Optional<File> metricsFile = findLatestMetricsFile( dbDir, metric );
            if ( metricsFile.isPresent() )
            {
                return metricsFile.get();
            }
            LockSupport.parkNanos( MILLISECONDS.toNanos( 10 ) );
        }
    }

    private static Optional<File> findLatestMetricsFile( File metricsPath, String metric )
    {
        String[] metricFiles = metricsPath.list( ( dir, name ) -> name.equals( metric ) );
        if ( isNotEmpty( metricFiles ) )
        {
            return Optional.of( new File( metricsPath, metricFiles[0] ) );
        }
        return Optional.empty();
    }

}
