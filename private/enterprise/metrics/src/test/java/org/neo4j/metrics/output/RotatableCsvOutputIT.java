/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.output;

import com.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.function.BiPredicate;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.metrics.source.db.TransactionMetrics;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.time.Duration.ofMinutes;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.fail;
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
    // this threshold should be bigger then size of file file header in bytes but smaller then file 2 lines
    private static final String FILE_SIZE_THRESHOLD = "54";

    @BeforeEach
    void setup()
    {
        outputPath = testDirectory.directory( "metrics" );
        database = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDirectory.storeDir() )
                .setConfig( csvPath, outputPath.getAbsolutePath() )
                .setConfig( csvRotationThreshold, FILE_SIZE_THRESHOLD )
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
        assertTimeoutPreemptively( ofMinutes( 2 ), () ->
        {
            // Commit a transaction and wait for rotation to happen
            doTransaction();
            waitForRotation( outputPath, TransactionMetrics.TX_COMMITTED );

            // Latest file should now have recorded the transaction
            File metricsFile = metricsCsv( outputPath, TransactionMetrics.TX_COMMITTED );
            long committedTransactions = readLongCounterAndAssert( metricsFile, MONOTONIC );
            assertEquals( 1, committedTransactions );

            // Commit yet another transaction and wait for rotation to happen again
            doTransaction();
            waitForRotation( outputPath, TransactionMetrics.TX_COMMITTED );

            // Latest file should now have recorded the new transaction
            File metricsFile2 = metricsCsv( outputPath, TransactionMetrics.TX_COMMITTED );
            long committedTransactions2 = readLongCounterAndAssert( metricsFile2, MONOTONIC );
            assertEquals( 2, committedTransactions2 );
        } );
    }

    private void doTransaction()
    {
        try ( Transaction transaction = database.beginTx() )
        {
            database.createNode();
            transaction.success();
        }
    }

    private static void waitForRotation( File dbDir, String metric ) throws InterruptedException
    {
        // Find highest missing file
        int i = 0;
        while ( getMetricFile( dbDir, metric, i ).exists() )
        {
            i++;
        }

        if ( i >= MAX_ARCHIVES )
        {
            fail( "Test did not finish before " + MAX_ARCHIVES + " rotations, which means we have rotated away from the " +
                    "file we want to assert on." );
        }

        // wait for file to exists
        metricsCsv( dbDir, metric, i );
    }

    private static File metricsCsv( File dbDir, String metric ) throws InterruptedException
    {
        return metricsCsv( dbDir, metric, 0 );
    }

    private static File metricsCsv( File dbDir, String metric, long index ) throws InterruptedException
    {
        File csvFile = getMetricFile( dbDir, metric, index );
        assertEventually( "Metrics file should exist", csvFile::exists, is( true ), 40, SECONDS );
        return csvFile;
    }

    private static File getMetricFile( File dbDir, String metric, long index )
    {
        return new File( dbDir, index > 0 ? metric + ".csv." + index : metric + ".csv" );
    }
}
