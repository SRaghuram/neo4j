/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics;

import com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;
import com.neo4j.test.extension.CommercialDbmsExtension;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.UUID;
import javax.management.MBeanServer;

import org.neo4j.collection.RawIterator;
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

import static com.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static com.neo4j.metrics.MetricsTestHelper.readLongGaugeAndAssert;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.api.ResourceManager.EMPTY_RESOURCE_MANAGER;
import static org.neo4j.values.storable.Values.stringValue;

@CommercialDbmsExtension( configurationCallback = "configure" )
class GlobalMetricsExtensionFactoryIT
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
        builder.setConfig( MetricsSettings.metricsEnabled, TRUE );
        builder.setConfig( MetricsSettings.jmxEnabled, TRUE );
        builder.setConfig( MetricsSettings.csvEnabled, TRUE );
        builder.setConfig( GraphDatabaseSettings.cypher_min_replan_interval, "0m" );
        builder.setConfig( MetricsSettings.csvPath, outputPath.getAbsolutePath() );
        builder.setConfig( GraphDatabaseSettings.check_point_interval_time, "100ms" );
        builder.setConfig( MetricsSettings.graphiteInterval, "1s" );
        builder.setConfig( GraphDatabaseSettings.record_id_batch_size, "1" );
        builder.setConfig( OnlineBackupSettings.online_backup_enabled, FALSE );
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
        assertThat( threadTotalResult, greaterThanOrEqualTo( 0L ) );
        assertThat( threadCountResult, greaterThanOrEqualTo( 0L ) );
    }

    @Test
    void mustBeAbleToStartWithNullTracer()
    {
        // Start the database
        File disabledTracerDb = directory.databaseDir( "disabledTracerDb" );

        DatabaseManagementService managementService = new TestCommercialDatabaseManagementServiceBuilder( disabledTracerDb )
                .setConfig( MetricsSettings.metricsEnabled, TRUE )
                .setConfig( MetricsSettings.csvEnabled, TRUE )
                .setConfig( MetricsSettings.csvPath, outputPath.getAbsolutePath() )
                .setConfig( GraphDatabaseSettings.tracer, "null" ) // key point!
                .setConfig( OnlineBackupSettings.online_backup_enabled, FALSE )
                .build();
        // key point!
        GraphDatabaseService nullTracerDatabase = managementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction tx = nullTracerDatabase.beginTx() )
        {
            Node node = nullTracerDatabase.createNode();
            node.setProperty( "all", "is well" );
            tx.success();
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
        RawIterator<AnyValue[],ProcedureException> result = procedure.apply( null, new AnyValue[]{jmxQuery}, EMPTY_RESOURCE_MANAGER );

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
            return item.length > 2 && stringValue( "neo4j.metrics:name=neo4j.system.transaction.active_write" ).equals( item[0] ) &&
                    stringValue( "Information on the management interface of the MBean" ).equals( item[1] );
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "Expected to see neo4j.system.transaction.active_write in result set" );
        }
    }
}
