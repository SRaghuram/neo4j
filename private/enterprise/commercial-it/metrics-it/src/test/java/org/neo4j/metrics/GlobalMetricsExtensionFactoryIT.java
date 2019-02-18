/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics;

import com.neo4j.commercial.edition.factory.CommercialGraphDatabaseFactory;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.test.rule.CommercialDbmsRule;
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
import javax.management.MBeanServer;

import org.neo4j.collection.RawIterator;
import org.neo4j.configuration.Settings;
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
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static org.neo4j.metrics.MetricsTestHelper.readLongGaugeAndAssert;
import static org.neo4j.values.storable.Values.stringValue;

public class GlobalMetricsExtensionFactoryIT
{
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory();

    @Rule
    public final DbmsRule dbRule = new CommercialDbmsRule( directory ).startLazily();

    private File outputPath;
    private GraphDatabaseAPI db;
    private final ResourceTracker resourceTracker = new StubResourceManager();

    @Before
    public void setup()
    {
        outputPath = new File( directory.storeDir(), "metrics" );
        Map<Setting<?>, String> config = new HashMap<>();
        config.put( MetricsSettings.neoEnabled, Settings.TRUE );
        config.put( MetricsSettings.metricsEnabled, Settings.TRUE );
        config.put( MetricsSettings.jmxEnabled, Settings.TRUE );
        config.put( MetricsSettings.csvEnabled, Settings.TRUE );
        config.put( GraphDatabaseSettings.cypher_min_replan_interval, "0m" );
        config.put( MetricsSettings.csvPath, outputPath.getAbsolutePath() );
        config.put( GraphDatabaseSettings.check_point_interval_time, "100ms" );
        config.put( MetricsSettings.graphiteInterval, "1s" );
        config.put( GraphDatabaseSettings.record_id_batch_size, "1" );
        config.put( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
        db = dbRule.withSettings( config ).getGraphDatabaseAPI();
        addNodes( 1 ); // to make sure creation of label and property key tokens do not mess up with assertions in tests
    }

    @Test
    public void shouldShowMetricsForThreads() throws Throwable
    {
        // WHEN
        addNodes( 100 );

        // wait for the file to be written before shutting down the cluster
        File threadTotalFile = metricsCsv( outputPath, "neo4j.vm.thread.total" );
        File threadCountFile = metricsCsv( outputPath, "neo4j.vm.thread.count" );

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
        GraphDatabaseBuilder builder = new CommercialGraphDatabaseFactory().newEmbeddedDatabaseBuilder( disabledTracerDb );
        GraphDatabaseService nullTracerDatabase =
                builder.setConfig( MetricsSettings.neoEnabled, Settings.TRUE ).setConfig( MetricsSettings.csvEnabled, Settings.TRUE )
                        .setConfig( MetricsSettings.csvPath, outputPath.getAbsolutePath() )
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

        TextValue jmxQuery = stringValue( "neo4j.metrics:*" );
        RawIterator<AnyValue[],ProcedureException> result = procedure.apply( null, new AnyValue[]{jmxQuery}, resourceTracker );

        List<AnyValue[]> queryResult = asList( result );
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

    private static class MetricsRecordMatcher extends TypeSafeMatcher<AnyValue[]>
    {
        @Override
        protected boolean matchesSafely( AnyValue[] item )
        {
            return item.length > 2 && stringValue( "neo4j.metrics:name=neo4j.system.db.transaction.active_write" ).equals( item[0] ) &&
                    stringValue( "Information on the management interface of the MBean" ).equals( item[1] );
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "Expected to see neo4j.vm.memory.pool.code_cache in result set" );
        }
    }
}
