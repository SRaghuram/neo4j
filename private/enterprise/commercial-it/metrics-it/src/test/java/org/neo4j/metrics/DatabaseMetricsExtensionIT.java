/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.test.extension.CommercialDbmsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Settings;
import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.metrics.global.MetricsManager;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.System.currentTimeMillis;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_interval_time;
import static org.neo4j.configuration.GraphDatabaseSettings.cypher_min_replan_interval;
import static org.neo4j.configuration.GraphDatabaseSettings.record_id_batch_size;
import static org.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static org.neo4j.metrics.MetricsTestHelper.readLongCounterAndAssert;
import static org.neo4j.metrics.MetricsTestHelper.readLongGaugeAndAssert;

@CommercialDbmsExtension( configurationCallback = "configure" )
class DatabaseMetricsExtensionIT
{
    @Inject
    private TestDirectory directory;

    @Inject
    private GraphDatabaseAPI db;

    private File outputPath;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        outputPath = new File( directory.storeDir(), "metrics" );
        builder.setConfig( MetricsSettings.metricsEnabled, Settings.TRUE );
        builder.setConfig( MetricsSettings.csvEnabled, Settings.TRUE );
        builder.setConfig( cypher_min_replan_interval, "0m" );
        builder.setConfig( MetricsSettings.csvPath, outputPath.getAbsolutePath() );
        builder.setConfig( check_point_interval_time, "100ms" );
        builder.setConfig( MetricsSettings.graphiteInterval, "1s" );
        builder.setConfig( record_id_batch_size, "1" );
        builder.setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
    }

    @BeforeEach
    void setup()
    {
        addNodes( 1 ); // to make sure creation of label and property key tokens do not mess up with assertions in tests
    }

    @Test
    void shouldShowTxCommittedMetricsWhenMetricsEnabled() throws Throwable
    {
        // GIVEN
        long lastCommittedTransactionId = db.getDependencyResolver().resolveDependency( TransactionIdStore.class )
                .getLastCommittedTransactionId();

        // Create some activity that will show up in the metrics data.
        addNodes( 1000 );
        File metricsFile = metricsCsv( outputPath, "neo4j." + db.databaseLayout().getDatabaseName() + ".transaction.committed" );

        // WHEN
        // We should at least have a "timestamp" column, and a "neo4j.transaction.committed" column
        long committedTransactions = readLongCounterAndAssert( metricsFile,
                ( newValue, currentValue ) -> newValue >= currentValue );

        // THEN
        assertThat( committedTransactions, greaterThanOrEqualTo( lastCommittedTransactionId ) );
        assertThat( committedTransactions, lessThanOrEqualTo( lastCommittedTransactionId + 1001L ) );
    }

    @Test
    void shouldShowEntityCountMetricsWhenMetricsEnabled() throws Throwable
    {
        // GIVEN
        // Create some activity that will show up in the metrics data.
        addNodes( 1000 );
        File metricsFile = metricsCsv( outputPath, "neo4j." + db.databaseLayout().getDatabaseName() + ".ids_in_use.node" );

        // WHEN
        // We should at least have a "timestamp" column, and a "neo4j.transaction.committed" column
        long committedTransactions = readLongGaugeAndAssert( metricsFile,
                ( newValue, currentValue ) -> newValue >= currentValue );

        // THEN
        assertThat( committedTransactions, lessThanOrEqualTo( 1001L ) );
    }

    @Test
    void showReplanEvents() throws Throwable
    {
        // GIVEN
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "match (n:Label {name: 'Pontus'}) return n.name" ).close();
            tx.success();
        }

        //add some data, should make plan stale
        addNodes( 100 );

        // WHEN
        for ( int i = 0; i < 10; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.execute( "match (n:Label {name: 'Pontus'}) return n.name" ).close();
                tx.success();
            }
            addNodes( 1 );
        }

        File replanCountMetricFile = metricsCsv( outputPath, "neo4j." + db.databaseLayout().getDatabaseName() + ".cypher.replan_events" );
        File replanWaitMetricFile = metricsCsv( outputPath, "neo4j." + db.databaseLayout().getDatabaseName() + ".cypher.replan_wait_time" );

        // THEN see that the replan metric have pickup up at least one replan event
        // since reporting happens in an async fashion then give it some time and check now and then
        long endTime = currentTimeMillis() + TimeUnit.SECONDS.toMillis( 10 );
        long events = 0;
        while ( currentTimeMillis() < endTime && events == 0 )
        {
            readLongCounterAndAssert( replanWaitMetricFile, ( newValue, currentValue ) -> newValue >= currentValue );
            events = readLongCounterAndAssert( replanCountMetricFile, ( newValue, currentValue ) -> newValue >= currentValue );
            if ( events == 0 )
            {
                Thread.sleep( 300 );
            }
        }
        assertThat( events, greaterThan( 0L ) );
    }

    @Test
    void shouldUseEventBasedReportingCorrectly() throws Throwable
    {
        // GIVEN
        addNodes( 100 );

        // WHEN
        CheckPointer checkPointer = db.getDependencyResolver().resolveDependency( CheckPointer.class );
        checkPointer.checkPointIfNeeded( new SimpleTriggerInfo( "test" ) );

        // wait for the file to be written before shutting down the cluster
        File metricFile = metricsCsv( outputPath, "neo4j." + db.databaseLayout().getDatabaseName() + ".check_point.duration" );

        long result = readLongGaugeAndAssert( metricFile, ( newValue, currentValue ) -> newValue >= 0 );

        // THEN
        assertThat( result, greaterThanOrEqualTo( 0L ) );
    }

    @Test
    void registerDatabaseMetricsOnDatabaseStart() throws DatabaseExistsException
    {
        DatabaseManager<?> databaseManager = db.getDependencyResolver().resolveDependency( DatabaseManager.class );
        MetricsManager metricsManager = db.getDependencyResolver().resolveDependency( MetricsManager.class );

        assertThat( metricsManager.getRegistry().getNames(), not( hasItem( "neo4j.testdb.check_point.events" ) ) );

        DatabaseId testdb = new DatabaseId( "testdb" );
        databaseManager.createDatabase( testdb );

        assertThat( metricsManager.getRegistry().getNames(), hasItem( "neo4j.testdb.check_point.events" ) );
        databaseManager.dropDatabase( testdb );
    }

    @Test
    void removeDatabaseMetricsOnDatabaseStop() throws DatabaseExistsException, DatabaseNotFoundException
    {
        DatabaseManager<?> databaseManager = db.getDependencyResolver().resolveDependency( DatabaseManager.class );
        MetricsManager metricsManager = db.getDependencyResolver().resolveDependency( MetricsManager.class );

        DatabaseId testDbName = new DatabaseId( "testdb" );
        databaseManager.createDatabase( testDbName );
        assertThat( metricsManager.getRegistry().getNames(), hasItem( "neo4j.testdb.check_point.events" ) );

        databaseManager.stopDatabase( testDbName );
        assertThat( metricsManager.getRegistry().getNames(), not( hasItem( "neo4j.testdb.check_point.events" ) ) );
        databaseManager.dropDatabase( testDbName );
    }

    private void addNodes( int numberOfNodes )
    {
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( Label.label( "Label" ) );
                node.setProperty( "name", UUID.randomUUID().toString() );
                tx.success();
            }
        }
    }
}
