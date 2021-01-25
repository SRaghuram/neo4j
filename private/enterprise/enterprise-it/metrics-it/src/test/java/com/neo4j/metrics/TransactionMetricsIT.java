/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics;

import com.neo4j.configuration.MetricsSettings;
import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;

import org.neo4j.configuration.helpers.GlobbingPattern;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.conditions.Conditions;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static com.neo4j.metrics.MetricsTestHelper.readHistogramCountValue;
import static com.neo4j.metrics.MetricsTestHelper.readHistogramMeanValue;
import static com.neo4j.metrics.MetricsTestHelper.readLongCounterAndAssert;
import static com.neo4j.metrics.MetricsTestHelper.readLongGaugeValue;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;
import static org.neo4j.test.assertion.Assert.assertEventually;

@EnterpriseDbmsExtension( configurationCallback = "configure" )
public class TransactionMetricsIT
{
    @Inject
    private TestDirectory directory;
    @Inject
    private GraphDatabaseAPI database;
    private Path metricsDirectory;
    private final Condition<Long> zero = Conditions.equalityCondition( 0L );
    private final Condition<Long> greaterThanZero = new Condition<>( value -> value > 0, "Greater than zero" );
    private final Condition<Long> greaterThanOne = new Condition<>( value -> value > 1, "Greater than one" );

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        metricsDirectory = directory.homePath( "metrics" );
        builder.setConfig( MetricsSettings.metrics_enabled, true )
                .setConfig( MetricsSettings.metrics_filter, GlobbingPattern.create( "*.transaction.*" ) )
                .setConfig( MetricsSettings.csv_enabled, true )
                .setConfig( MetricsSettings.csv_interval, Duration.ofMillis( 10 ) )
                .setConfig( preallocate_logical_logs, false )
                .setConfig( MetricsSettings.csv_path, metricsDirectory.toAbsolutePath() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, false );
    }

    @Test
    void transactionCommittedMetrics()
    {
        String committed = createFullMetricName( "committed" );
        String committedRead = createFullMetricName( "committed_read" );
        String committedWrite = createFullMetricName( "committed_write" );
        String lastCommittedId = createFullMetricName( "last_committed_tx_id" );
        String lastClosedId = createFullMetricName( "last_closed_tx_id" );

        // Commit a write transaction
        Label testLabel = Label.label( "testLabel" );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = transaction.createNode( testLabel );
            node.setProperty( "property", "value" );
            transaction.commit();
        }

        assertMetrics( "Nbr committed txs should be updated", committed, greaterThanZero );
        assertMetrics( "Nbr committed read txs should still be 0", committedRead, zero );
        assertMetrics( "Nbr committed write txs should be updated", committedWrite, greaterThanZero );

        // Commit a read transaction
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.findNode( testLabel, "property", "value" );
            transaction.commit();
        }

        assertMetrics( "Nbr committed txs should still be greater than 0", committed, greaterThanZero );
        assertMetrics( "Nbr committed read txs should be updated", committedRead, greaterThanZero );
        assertMetrics( "Nbr committed write txs should still be greater than 0", committedWrite, greaterThanZero );
        assertMetrics( "Last committed tx id should be set", lastCommittedId, greaterThanZero );
        assertMetrics( "Last closed tx id should be set", lastClosedId, greaterThanZero );
    }

    @Test
    void transactionRollbacksMetrics()
    {
        String rollback = createFullMetricName( "rollbacks" );
        String rollbackRead = createFullMetricName( "rollbacks_read" );
        String rollbackWrite = createFullMetricName( "rollbacks_write" );

        // Rollback a write transaction
        Label testLabel = Label.label( "testLabel" );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = transaction.createNode( testLabel );
            node.setProperty( "property", "value" );
        }

        assertMetrics( "Nbr rollback txs should be updated", rollback, greaterThanZero );
        assertMetrics( "Nbr rollback read txs should still be 0", rollbackRead, zero );
        assertMetrics( "Nbr rollback write txs should be updated", rollbackWrite, greaterThanZero );

        // Rollback a read transaction
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.findNode( testLabel, "property", "value" );
        }

        assertMetrics( "Nbr rollback txs should still be greater than 0", rollback, greaterThanZero );
        assertMetrics( "Nbr rollback read txs should be updated", rollbackRead, greaterThanZero );
        assertMetrics( "Nbr rollback write txs should still be greater than 0", rollbackWrite, greaterThanZero );
    }

    @Test
    void transactionActiveStartedMetrics()
    {
        String active = createFullMetricName( "active" );
        String activeRead = createFullMetricName( "active_read" );
        String activeWrite = createFullMetricName( "active_write" );
        String lastCommittedId = createFullMetricName( "last_committed_tx_id" );
        String lastClosedId = createFullMetricName( "last_closed_tx_id" );
        String started = createFullMetricName( "started" );
        String peakCount = createFullMetricName( "peak_concurrent" );

        // A write transaction
        Label testLabel = Label.label( "testLabel" );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = transaction.createNode( testLabel );
            node.setProperty( "property", "value" );

            assertGauge("Nbr active txs should be updated", active, greaterThanZero );
            assertGauge( "Nbr active read txs should still be 0", activeRead, zero );
            assertGauge( "Nbr active write txs should be updated", activeWrite, greaterThanZero );

            transaction.commit();
        }

        // A read transaction
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.findNode( testLabel, "property", "value" );

            assertGauge("Nbr active txs should still be greater than 0", active, greaterThanZero );
            assertGauge( "Nbr active read txs should be updated", activeRead, greaterThanZero );
            assertGauge( "Nbr active write txs should be 0 again", activeWrite, zero );

            transaction.commit();
        }

        assertMetrics( "Last committed tx id should be set", lastCommittedId, greaterThanZero );
        assertMetrics( "Last closed tx id should be set", lastClosedId, greaterThanZero );
        assertMetrics( "Nbr started txs should be updated", started, greaterThanOne );
        assertMetrics( "Peak concurrent txs should be updated", peakCount, greaterThanZero );
    }

    @Test
    void transactionSizeMetrics()
    {
        String sizeHeap = createFullMetricName( "tx_size_heap" );
        String sizeNative = createFullMetricName( "tx_size_native" );

        // A write transaction
        Label testLabel = Label.label( "testLabel" );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = transaction.createNode( testLabel );
            node.setProperty( "property", "value" );

            transaction.commit();
        }

        assertHistogramCount("Native transaction size should have been updated", sizeNative,
                new Condition<>( value -> value > 0, "Greater than zero" ) );
        assertHistogramMeanValue("Heap transaction size should have been updated", sizeHeap,
                new Condition<>( value -> value > 0.0, "Greater than zero" ) );
    }

    private void assertMetrics( String message, String metricName, Condition<Long> condition )
    {
        assertEventually( message, () -> readLongCounterAndAssert( metricsCsv( metricsDirectory, metricName ), -1, ( a, b ) -> true ), condition, 1, MINUTES );
    }

    private void assertGauge( String message, String metricName, Condition<Long> condition )
    {
        assertEventually( message, () -> readLongGaugeValue( metricsCsv( metricsDirectory, metricName ) ), condition, 1, MINUTES );
    }

    private void assertHistogramCount( String message, String metricName, Condition<Integer> condition )
    {
        assertEventually( message, () -> readHistogramCountValue( metricsCsv( metricsDirectory, metricName ) ), condition, 1, MINUTES );
    }

    private void assertHistogramMeanValue( String message, String metricName, Condition<Double> condition )
    {
        assertEventually( message, () -> readHistogramMeanValue( metricsCsv( metricsDirectory, metricName ) ), condition, 1, MINUTES );
    }

    private String createFullMetricName( String metricName )
    {
        return "neo4j." + database.databaseName() + ".transaction." + metricName;
    }
}
