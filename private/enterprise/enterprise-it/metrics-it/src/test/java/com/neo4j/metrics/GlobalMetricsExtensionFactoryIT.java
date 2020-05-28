/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics;

import com.neo4j.configuration.MetricsSettings;
import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import javax.management.MBeanServer;

import org.neo4j.collection.RawIterator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.builtin.JmxQueryProcedure;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;

import static com.neo4j.configuration.MetricsSettings.metrics_prefix;
import static com.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static com.neo4j.metrics.MetricsTestHelper.readLongGaugeAndAssert;
import static com.neo4j.metrics.source.jvm.HeapMetrics.HEAP_COMMITTED_TEMPLATE;
import static com.neo4j.metrics.source.jvm.HeapMetrics.HEAP_MAX_TEMPLATE;
import static com.neo4j.metrics.source.jvm.HeapMetrics.HEAP_USED_TEMPLATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.api.ResourceTracker.EMPTY_RESOURCE_TRACKER;
import static org.neo4j.values.storable.Values.stringValue;

@EnterpriseDbmsExtension( configurationCallback = "configure" )
class GlobalMetricsExtensionFactoryIT
{
    @Inject
    private TestDirectory directory;

    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private Config config;

    private File outputPath;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        outputPath = new File( directory.homeDir(), "metrics" );
        builder.setConfig( MetricsSettings.metrics_enabled, true );
        builder.setConfig( MetricsSettings.jmx_enabled, true );
        builder.setConfig( MetricsSettings.csv_enabled, true );
        builder.setConfig( GraphDatabaseSettings.cypher_min_replan_interval, Duration.ofMillis( 0 ) );
        builder.setConfig( MetricsSettings.csv_path, outputPath.toPath().toAbsolutePath() );
        builder.setConfig( GraphDatabaseSettings.check_point_interval_time, Duration.ofMillis( 100 ) );
        builder.setConfig( MetricsSettings.graphite_interval, Duration.ofSeconds( 1 ) );
        builder.setConfig( OnlineBackupSettings.online_backup_enabled, false );
    }

    @BeforeEach
    void setup()
    {
        addNodes( 1 ); // to make sure creation of label and property key tokens do not mess up with assertions in tests
    }

    @Test
    void shouldShowMetricsForThreads() throws Throwable
    {
        // WHEN
        addNodes( 100 );

        // wait for the file to be written before shutting down the cluster
        File threadTotalFile = metricsCsv( outputPath, "neo4j.vm.thread.total" );
        File threadCountFile = metricsCsv( outputPath, "neo4j.vm.thread.count" );

        long threadTotalResult = readLongGaugeAndAssert( threadTotalFile, ( newValue, currentValue ) -> newValue >= 0 );
        long threadCountResult = readLongGaugeAndAssert( threadCountFile, ( newValue, currentValue ) -> newValue >= 0 );

        // THEN
        assertThat( threadTotalResult ).isGreaterThanOrEqualTo( 0L );
        assertThat( threadCountResult ).isGreaterThanOrEqualTo( 0L );
    }

    @Test
    void reportHeapUsageMetrics() throws IOException
    {
        String prefix = config.get( metrics_prefix ) + ".";
        File heapCommittedFile = metricsCsv( outputPath, prefix + HEAP_COMMITTED_TEMPLATE );
        File heapMaxFile = metricsCsv( outputPath, prefix + HEAP_MAX_TEMPLATE );
        File heapUsedFile = metricsCsv( outputPath, prefix + HEAP_USED_TEMPLATE );

        long heapCommittedResult = readLongGaugeAndAssert( heapCommittedFile, ( newValue, currentValue ) -> newValue >= 0 );
        long heapUsedResult = readLongGaugeAndAssert( heapUsedFile, ( newValue, currentValue ) -> newValue >= 0 );
        long heapMaxResult = readLongGaugeAndAssert( heapMaxFile, ( newValue, currentValue ) -> newValue >= 0 );

        // THEN
        assertThat( heapCommittedResult ).isGreaterThanOrEqualTo( 0L );
        assertThat( heapUsedResult ).isGreaterThanOrEqualTo( 0L );
        assertThat( heapMaxResult ).isGreaterThanOrEqualTo( 0L );
    }

    @Test
    void mustBeAbleToStartWithNullTracer()
    {
        // Start the database
        File disabledTracerDb = directory.homeDir( "disabledTracerDb" );

        DatabaseManagementService managementService = new TestEnterpriseDatabaseManagementServiceBuilder( disabledTracerDb )
                .setConfig( MetricsSettings.metrics_enabled, true )
                .setConfig( MetricsSettings.csv_enabled, true )
                .setConfig( MetricsSettings.jmx_enabled, false )
                .setConfig( MetricsSettings.csv_path, outputPath.toPath().toAbsolutePath() )
                .setConfig( GraphDatabaseInternalSettings.tracer, "null" ) // key point!
                .setConfig( OnlineBackupSettings.online_backup_enabled, false )
                .build();
        // key point!
        GraphDatabaseService nullTracerDatabase = managementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction tx = nullTracerDatabase.beginTx() )
        {
            Node node = tx.createNode();
            node.setProperty( "all", "is well" );
            tx.commit();
        }
        finally
        {
            managementService.shutdown();
        }
        // We assert that no exception is thrown during startup or the operation of the database.
    }

    @Test
    void metricsAccessibleOverJmx() throws ProcedureException
    {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        QualifiedName qualifiedName = ProcedureSignature.procedureName( "metricsQuery" );
        JmxQueryProcedure procedure = new JmxQueryProcedure( qualifiedName, mBeanServer );

        TextValue jmxQuery = stringValue( "neo4j.metrics:*" );
        RawIterator<AnyValue[],ProcedureException> result = procedure.apply( null, new AnyValue[]{jmxQuery}, EMPTY_RESOURCE_TRACKER );

        List<AnyValue[]> queryResult = asList( result );
        assertThat( queryResult ).has( new MetricsRecordCondition() );
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

    private static class MetricsRecordCondition extends Condition<List<? extends AnyValue[]>>
    {
        MetricsRecordCondition()
        {
            super( ( List<? extends AnyValue[]> items ) ->
            {
                for ( AnyValue[] item : items )
                {
                    if ( item.length > 2 &&
                            stringValue( "neo4j.metrics:name=neo4j.system.transaction.active_write" ).equals( item[0] ) &&
                            stringValue( "Information on the management interface of the MBean" ).equals( item[1] ) )
                    {
                        return true;
                    }
                }
                return false;
            }, "Expected to see neo4j.system.transaction.active_write in result set" );
        }
    }
}
