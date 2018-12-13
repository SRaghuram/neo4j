/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics;

import com.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.test.rule.EnterpriseDbmsRule;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;

import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.StubResourceManager;
import org.neo4j.kernel.builtinprocs.JmxQueryProcedure;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.metrics.source.db.CheckPointingMetrics;
import org.neo4j.metrics.source.db.CypherMetrics;
import org.neo4j.metrics.source.db.EntityCountMetrics;
import org.neo4j.metrics.source.db.TransactionMetrics;
import org.neo4j.metrics.source.jvm.ThreadMetrics;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.System.currentTimeMillis;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.check_point_interval_time;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.cypher_min_replan_interval;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_id_batch_size;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.metrics.MetricsSettings.csvEnabled;
import static org.neo4j.metrics.MetricsSettings.csvPath;
import static org.neo4j.metrics.MetricsSettings.graphiteInterval;
import static org.neo4j.metrics.MetricsSettings.metricsEnabled;
import static org.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static org.neo4j.metrics.MetricsTestHelper.readLongCounterAndAssert;
import static org.neo4j.metrics.MetricsTestHelper.readLongGaugeAndAssert;

public class MetricsKernelExtensionFactoryIT
{
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory();

    @Rule
    public final DbmsRule dbRule = new EnterpriseDbmsRule( directory ).startLazily();

    private File outputPath;
    private GraphDatabaseAPI db;
    private final ResourceTracker resourceTracker = new StubResourceManager();

    @Before
    public void setup()
    {
        outputPath = new File( directory.databaseDir(), "metrics" );
        Map<Setting<?>, String> config = new HashMap<>();
        config.put( MetricsSettings.neoEnabled, Settings.TRUE );
        config.put( metricsEnabled, Settings.TRUE );
        config.put( csvEnabled, Settings.TRUE );
        config.put( cypher_min_replan_interval, "0m" );
        config.put( csvPath, outputPath.getAbsolutePath() );
        config.put( check_point_interval_time, "100ms" );
        config.put( graphiteInterval, "1s" );
        config.put( record_id_batch_size, "1" );
        config.put( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
        db = dbRule.withSettings( config ).getGraphDatabaseAPI();
        addNodes( 1 ); // to make sure creation of label and property key tokens do not mess up with assertions in tests
    }

    @Test
    public void shouldShowTxCommittedMetricsWhenMetricsEnabled() throws Throwable
    {
        // GIVEN
        long lastCommittedTransactionId = db.getDependencyResolver().resolveDependency( TransactionIdStore.class )
                .getLastCommittedTransactionId();

        // Create some activity that will show up in the metrics data.
        addNodes( 1000 );
        File metricsFile = metricsCsv( outputPath, TransactionMetrics.TX_COMMITTED );

        // WHEN
        // We should at least have a "timestamp" column, and a "neo4j.transaction.committed" column
        long committedTransactions = readLongCounterAndAssert( metricsFile,
                ( newValue, currentValue ) -> newValue >= currentValue );

        // THEN
        assertThat( committedTransactions, greaterThanOrEqualTo( lastCommittedTransactionId ) );
        assertThat( committedTransactions, lessThanOrEqualTo( lastCommittedTransactionId + 1001L ) );
    }

    @Test
    public void shouldShowEntityCountMetricsWhenMetricsEnabled() throws Throwable
    {
        // GIVEN
        // Create some activity that will show up in the metrics data.
        addNodes( 1000 );
        File metricsFile = metricsCsv( outputPath, EntityCountMetrics.COUNTS_NODE );

        // WHEN
        // We should at least have a "timestamp" column, and a "neo4j.transaction.committed" column
        long committedTransactions = readLongGaugeAndAssert( metricsFile,
                ( newValue, currentValue ) -> newValue >= currentValue );

        // THEN
        assertThat( committedTransactions, lessThanOrEqualTo( 1001L ) );
    }

    @Test
    public void showReplanEvents() throws Throwable
    {
        // GIVEN
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "match (n:Label {name: 'Pontus'}) return n.name" ).close();
            tx.success();
        }

        //add some data, should make plan stale
        addNodes( 10 );

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

        File replanCountMetricFile = metricsCsv( outputPath, CypherMetrics.REPLAN_EVENTS );
        File replanWaitMetricFile = metricsCsv( outputPath, CypherMetrics.REPLAN_WAIT_TIME );

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
    public void shouldUseEventBasedReportingCorrectly() throws Throwable
    {
        // GIVEN
        addNodes( 100 );

        // WHEN
        CheckPointer checkPointer = db.getDependencyResolver().resolveDependency( CheckPointer.class );
        checkPointer.checkPointIfNeeded( new SimpleTriggerInfo( "test" ) );

        // wait for the file to be written before shutting down the cluster
        File metricFile = metricsCsv( outputPath, CheckPointingMetrics.CHECK_POINT_DURATION );

        long result = readLongGaugeAndAssert( metricFile, ( newValue, currentValue ) -> newValue >= 0 );

        // THEN
        assertThat( result, greaterThanOrEqualTo( 0L ) );
    }

    @Test
    public void shouldShowMetricsForThreads() throws Throwable
    {
        // WHEN
        addNodes( 100 );

        // wait for the file to be written before shutting down the cluster
        File threadTotalFile = metricsCsv( outputPath, ThreadMetrics.THREAD_TOTAL );
        File threadCountFile = metricsCsv( outputPath, ThreadMetrics.THREAD_COUNT );

        long threadTotalResult = readLongGaugeAndAssert( threadTotalFile, ( newValue, currentValue ) -> newValue >= 0 );
        long threadCountResult = readLongGaugeAndAssert( threadCountFile, ( newValue, currentValue ) -> newValue >= 0 );

        // THEN
        assertThat( threadTotalResult, greaterThanOrEqualTo( 0L ) );
        assertThat( threadCountResult, greaterThanOrEqualTo( 0L ) );
    }

    @Test
    public void mustBeAbleToStartWithNullTracer()
    {
        // Start the database
        File disabledTracerDb = directory.databaseDir( "disabledTracerDb" );
        GraphDatabaseBuilder builder = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabaseBuilder( disabledTracerDb );
        GraphDatabaseService nullTracerDatabase =
                builder.setConfig( MetricsSettings.neoEnabled, Settings.TRUE ).setConfig( csvEnabled, Settings.TRUE )
                        .setConfig( csvPath, outputPath.getAbsolutePath() )
                        .setConfig( GraphDatabaseSettings.tracer, "null" ) // key point!
                        .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                        .newGraphDatabase();
        try ( Transaction tx = nullTracerDatabase.beginTx() )
        {
            Node node = nullTracerDatabase.createNode();
            node.setProperty( "all", "is well" );
            tx.success();
        }
        finally
        {
            nullTracerDatabase.shutdown();
        }
        // We assert that no exception is thrown during startup or the operation of the database.
    }

    @Test
    public void metricsAccessibleOverJmx() throws ProcedureException
    {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        QualifiedName qualifiedName = ProcedureSignature.procedureName( "metricsQuery" );
        JmxQueryProcedure procedure = new JmxQueryProcedure( qualifiedName, mBeanServer );

        String jmxQuery = "metrics." + GraphDatabaseSettings.DEFAULT_DATABASE_NAME + ":*";
        RawIterator<Object[],ProcedureException> result = procedure.apply( null, new Object[]{jmxQuery}, resourceTracker );

        List<Object[]> queryResult = asList( result );
        assertThat( queryResult, hasItem( new MetricsRecordMatcher() ) );
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

    private static class MetricsRecordMatcher extends TypeSafeMatcher<Object[]>
    {
        @Override
        protected boolean matchesSafely( Object[] item )
        {
            return item.length > 2 && "metrics.graph.db:name=neo4j.transaction.rollbacks_read".equals( item[0] ) &&
                    "Information on the management interface of the MBean".equals( item[1] );
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "Expected to see neo4j.transaction.rollbacks_read in result set" );
        }
    }
}
