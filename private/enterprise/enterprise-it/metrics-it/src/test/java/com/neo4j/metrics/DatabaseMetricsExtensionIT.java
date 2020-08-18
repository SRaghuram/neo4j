/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.neo4j.configuration.MetricsSettings;
import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.metrics.database.DatabaseMetricsExtension;
import com.neo4j.metrics.database.DatabaseMetricsExtensionFactory;
import com.neo4j.metrics.global.GlobalMetricsExtension;
import com.neo4j.metrics.global.GlobalMetricsExtensionFactory;
import com.neo4j.metrics.global.MetricsManager;
import com.neo4j.metrics.source.db.DatabaseCountMetrics;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static com.neo4j.metrics.MetricsTestHelper.readLongCounterAndAssert;
import static com.neo4j.metrics.MetricsTestHelper.readLongCounterValue;
import static com.neo4j.metrics.MetricsTestHelper.readLongGaugeAndAssert;
import static com.neo4j.metrics.MetricsTestHelper.readLongGaugeValue;
import static com.neo4j.metrics.source.db.DatabaseCountMetrics.COUNTS_NODE_TEMPLATE;
import static com.neo4j.metrics.source.db.DatabaseCountMetrics.COUNTS_RELATIONSHIP_TEMPLATE;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_interval_time;
import static org.neo4j.configuration.GraphDatabaseSettings.cypher_min_replan_interval;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@EnterpriseDbmsExtension( configurationCallback = "configure" )
class DatabaseMetricsExtensionIT
{
    @Inject
    private TestDirectory directory;

    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private CheckPointer checkPointer;
    @Inject
    private MetricsManager metricsManager;
    @Inject
    private TransactionIdStore transactionIdStore;

    @Inject
    private DatabaseManagementService managementService;

    private Path outputPath;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        outputPath = directory.homePath("metrics" );
        builder.setConfig( MetricsSettings.metrics_enabled, true );
        builder.setConfig( MetricsSettings.csv_enabled, true );
        builder.setConfig( MetricsSettings.csv_interval, Duration.ofMillis( 30 ) );
        builder.setConfig( cypher_min_replan_interval, Duration.ofMinutes( 0 ) );
        builder.setConfig( MetricsSettings.csv_path, outputPath.toAbsolutePath() );
        builder.setConfig( check_point_interval_time, Duration.ofMillis( 100 ) );
        builder.setConfig( MetricsSettings.graphite_interval, Duration.ofSeconds( 1 ) );
        builder.setConfig( OnlineBackupSettings.online_backup_enabled, false );
    }

    @BeforeEach
    void setup()
    {
        addNodes( 1 ); // to make sure creation of label and property key tokens do not mess up with assertions in tests
    }

    @Test
    void reportCheckpointMetrics() throws Exception
    {
        checkPointer.forceCheckPoint( new SimpleTriggerInfo( "testTrigger" ) );

        Path checkpointsMetricsFile = metricsCsv( outputPath, "neo4j." + db.databaseName() + ".check_point.events" );
        assertEventually( "Metrics report should have correct number of checkpoints.",
                () -> readLongCounterValue( checkpointsMetricsFile ), new Condition<>( value -> value >= 1L, "More than 1." ), 1, MINUTES );
    }

    @Test
    void tracePageCacheAccessOnCountMetricsEvaluation()
    {
        var registry = new MetricRegistry();
        var tempDatabaseName = "foo";
        managementService.createDatabase( tempDatabaseName );
        GraphDatabaseAPI fooDb = (GraphDatabaseAPI) managementService.database( tempDatabaseName );
        var suppliers = fooDb.getDependencyResolver().provideDependency( StoreEntityCounters.class );
        var pageCacheTracer = new DefaultPageCacheTracer();

        var countMetrics = new DatabaseCountMetrics( "prefix", registry, suppliers, pageCacheTracer );
        countMetrics.start();

        var gauges = registry.getGauges().values();
        gauges.forEach( Gauge::getValue );
        assertEquals( 2, gauges.size() );

        assertEquals( 2, pageCacheTracer.pins() );
        assertEquals( 2, pageCacheTracer.unpins() );
        assertEquals( 2, pageCacheTracer.hits() );
    }

    @Test
    void countNodesAndRelationships()
    {
        for ( int i = 0; i < 5; i++ )
        {
            connectTwoNodes();
        }

        // 10 nodes created in this test and 1 in setup
        assertMetrics( "Should get correct number of nodes from count store",
                "neo4j." + db.databaseName() + "." + COUNTS_NODE_TEMPLATE, equalityCondition( 11L ) );
        assertMetrics( "Should get correct number of relationships from count store",
                "neo4j." + db.databaseName() + "." + COUNTS_RELATIONSHIP_TEMPLATE, equalityCondition( 5L ) );
    }

    @Test
    void shouldShowTxCommittedMetricsWhenMetricsEnabled() throws Throwable
    {
        // GIVEN
        long lastCommittedTransactionId = transactionIdStore.getLastCommittedTransactionId();

        // Create some activity that will show up in the metrics data.
        addNodes( 1000 );
        Path metricsFile = metricsCsv( outputPath, "neo4j." + db.databaseName() + ".transaction.committed" );

        // WHEN
        // We should at least have a "timestamp" column, and a "neo4j.transaction.committed" column
        long committedTransactions = readLongCounterAndAssert( metricsFile,
                ( newValue, currentValue ) -> newValue >= currentValue );

        // THEN
        assertThat( committedTransactions ).isGreaterThanOrEqualTo( lastCommittedTransactionId );
        assertThat( committedTransactions ).isLessThanOrEqualTo( lastCommittedTransactionId + 1001L );
    }

    @Test
    void shouldShowEntityCountMetricsWhenMetricsEnabled() throws Throwable
    {
        // GIVEN
        // Create some activity that will show up in the metrics data.
        addNodes( 1000 );
        Path metricsFile = metricsCsv( outputPath, "neo4j." + db.databaseName() + ".ids_in_use.node" );

        // WHEN
        long committedTransactions = readLongGaugeAndAssert( metricsFile,
                ( newValue, currentValue ) -> newValue >= currentValue );

        // THEN
        assertThat( committedTransactions ).isLessThanOrEqualTo( 1001L );
    }

    @Test
    void reportTransactionLogsAppendedBytesWithDefaultAllocationConfig() throws IOException
    {
        addNodes( 100 );
        Path metricsFile = metricsCsv( outputPath, "neo4j." + db.databaseName() + ".log.appended_bytes" );

        long appendedBytes = 0;
        int attempts = 0;
        do
        {
            attempts += 1;
            appendedBytes += readLongCounterAndAssert( metricsFile,
                    ( newValue, currentValue ) -> newValue >= currentValue );
        }
        while ( attempts <= 5 && appendedBytes == 0 );

        // THEN
        assertThat( appendedBytes ).isGreaterThan( 0L );
    }

    @Test
    void showReplanEvents() throws Throwable
    {
        // GIVEN
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "match (n:Label {name: 'Pontus'}) return n.name" ).close();
            tx.commit();
        }

        //add some data, should make plan stale
        addNodes( 100 );

        // WHEN
        for ( int i = 0; i < 10; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.execute( "match (n:Label {name: 'Pontus'}) return n.name" ).close();
                tx.commit();
            }
            addNodes( 1 );
        }

        Path replanCountMetricFile = metricsCsv( outputPath, "neo4j." + db.databaseName() + ".cypher.replan_events" );
        Path replanWaitMetricFile = metricsCsv( outputPath, "neo4j." + db.databaseName() + ".cypher.replan_wait_time" );

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
        assertThat( events ).isGreaterThan( 0L );
    }

    @Test
    void shouldUseEventBasedReportingCorrectly() throws Throwable
    {
        // GIVEN
        addNodes( 100 );

        // WHEN
        checkPointer.checkPointIfNeeded( new SimpleTriggerInfo( "test" ) );

        // wait for the file to be written before shutting down the cluster
        Path metricFile = metricsCsv( outputPath, "neo4j." + db.databaseName() + ".check_point.duration" );

        long result = readLongGaugeAndAssert( metricFile, ( newValue, currentValue ) -> newValue >= 0 );

        // THEN
        assertThat( result ).isGreaterThanOrEqualTo( 0L );
    }

    @Test
    void registerDatabaseMetricsOnDatabaseStart() throws DatabaseExistsException
    {
        assertThat( metricsManager.getRegistry().getNames() ).doesNotContain( "neo4j.testdb.check_point.events" );

        managementService.createDatabase( "testDb" );

        assertThat( metricsManager.getRegistry().getNames() ).contains( "neo4j.testdb.check_point.events" );
        managementService.dropDatabase( "testDb" );
    }

    @Test
    void removeDatabaseMetricsOnDatabaseStop() throws DatabaseExistsException, DatabaseNotFoundException
    {
        managementService.createDatabase( "testDb" );
        assertThat( metricsManager.getRegistry().getNames() ).contains( "neo4j.testdb.check_point.events" );

        managementService.shutdownDatabase( "testDb" );
        assertThat( metricsManager.getRegistry().getNames() ).doesNotContain( "neo4j.testdb.check_point.events" );
        managementService.dropDatabase( "testDb" );
    }

    @Test
    void ensureGlobalMetricsOutliveDatabaseMetrics()
    {
        //Given
        AtomicBoolean globalMetricsAlive = new AtomicBoolean( true );
        AtomicInteger databaseMetricsStoppedBeforeGlobal = new AtomicInteger( 0 );

        var gmeSpy = new GlobalMetricsExtensionFactory()
        {
            @Override
            public Lifecycle newInstance( ExtensionContext context, Dependencies dependencies )
            {
                return new GlobalMetricsExtension( context, dependencies )
                {
                    @Override
                    public void shutdown()
                    {
                        globalMetricsAlive.set( false );
                        super.shutdown();
                    }
                };
            }
        };

        var dmeSpy = new DatabaseMetricsExtensionFactory()
        {
            @Override
            public Lifecycle newInstance( ExtensionContext context, Dependencies dependencies )
            {
                return new DatabaseMetricsExtension( context, dependencies )
                {
                    @Override
                    public void shutdown()
                    {
                        if ( globalMetricsAlive.get() )
                        {
                            databaseMetricsStoppedBeforeGlobal.incrementAndGet();
                        }
                        super.shutdown();
                    }
                };
            }
        };

        //When
        DatabaseManagementService dbms = new TestEnterpriseDatabaseManagementServiceBuilder( directory.homePath( "life" ) )
                .removeExtensions( ef -> ef instanceof GlobalMetricsExtensionFactory || ef instanceof DatabaseMetricsExtensionFactory )
                .addExtension( gmeSpy )
                .addExtension( dmeSpy )
                .build();
        dbms.shutdown();

        //Then
        assertEquals( 2, databaseMetricsStoppedBeforeGlobal.get() ); // default and system
    }

    private void connectTwoNodes()
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();
            node1.createRelationshipTo( node2, withName( "any" ) );
            tx.commit();
        }
    }

    private void assertMetrics( String message, String metricName, Condition<Long> matcher )
    {
        assertEventually( message, () -> readLongGaugeValue( metricsCsv( outputPath, metricName ) ), matcher, 5, TimeUnit.MINUTES );
    }

    private void addNodes( int numberOfNodes )
    {
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = tx.createNode( Label.label( "Label" ) );
                node.setProperty( "name", UUID.randomUUID().toString() );
                tx.commit();
            }
        }
    }
}
