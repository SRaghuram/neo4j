/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.neo4j.configuration.MetricsSettings;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.FakeClock;

import static com.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static com.neo4j.metrics.MetricsTestHelper.readLongGaugeValue;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

@DbmsExtension( configurationCallback = "configure" )
class StoreSizeMetricsIT
{
    private static final Condition<Long> GREATER_THAN_ZERO = new Condition<>( value -> value > 0L, "Should be greater than 0." );

    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DatabaseManagementService managementService;
    @Inject
    private CheckPointer checkPointer;

    private File metricsFolder;
    private FakeClock clock;

    @ExtensionCallback
    private void configure( TestDatabaseManagementServiceBuilder builder )
    {
        clock = new FakeClock();
        builder.setConfig( MetricsSettings.metrics_enabled, true );
        builder.setConfig( MetricsSettings.neo_store_size_enabled, true );
        builder.setClock( clock );
    }

    @BeforeEach
    void setUp()
    {
        metricsFolder = testDirectory.directory( "metrics" );
    }

    @AfterEach
    void cleanUp()
    {
        managementService.shutdown();
    }

    @Test
    void shouldMonitorStoreSizeForAllDatabases()
    {
        for ( String name : managementService.listDatabases() )
        {
            String msg = name + " store has some size at startup";
            String metricsName = String.format( "neo4j.%s.store.size.total", name );
            assertEventually( msg, () -> readLongGaugeValue( metricsCsv( metricsFolder, metricsName ) ), GREATER_THAN_ZERO, 1, MINUTES );
        }
    }

    @Test
    void shouldMonitorDatabaseSizeForAllDatabases()
    {
        for ( String name : managementService.listDatabases() )
        {
            String msg = name + " store has some size at startup";
            String metricsName = String.format( "neo4j.%s.store.size.database", name );
            assertEventually( msg, () -> readLongGaugeValue( metricsCsv( metricsFolder, metricsName ) ), GREATER_THAN_ZERO, 1, MINUTES );
        }
    }

    @Test
    void shouldGrowWhenAddingData() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );

        String firstMsg = db.databaseName() + " store has some size at startup";
        String metricsName = String.format( "neo4j.%s.store.size.total", db.databaseName() );

        checkPoint();
        tick();

        assertEventually( firstMsg, () -> readLongGaugeValue( metricsCsv( metricsFolder, metricsName ) ), GREATER_THAN_ZERO, 1, MINUTES );
        long size = readLongGaugeValue( metricsCsv( metricsFolder, metricsName ) );

        addSomeData( db );
        checkPoint();
        tick();

        String secondMsg = db.databaseName() + " store grown after adding data";
        assertEventually( secondMsg, () -> readLongGaugeValue( metricsCsv( metricsFolder, metricsName ) ), value -> value > size, 1, MINUTES );
    }

    private void checkPoint() throws IOException
    {
        checkPointer.forceCheckPoint( new SimpleTriggerInfo( "test" ) );
    }

    private void addSomeData( GraphDatabaseAPI db )
    {
        for ( int reps = 0; reps < 3; reps++ )
        {
            try ( var transaction = db.beginTx() )
            {
                Label label = Label.label( "Label" );
                RelationshipType relationshipType = RelationshipType.withName( "REL" );
                long[] largeValue = new long[1024];
                for ( int i = 0; i < 1000; i++ )
                {
                    Node node = transaction.createNode( label );
                    node.setProperty( "Niels", "Borh" );
                    node.setProperty( "Albert", largeValue );
                    for ( int j = 0; j < 30; j++ )
                    {
                        Relationship rel = node.createRelationshipTo( node, relationshipType );
                        rel.setProperty( "Max", "Planck" );
                    }
                }
                transaction.commit();
            }
        }
    }

    private void tick()
    {
        clock.forward( StoreSizeMetrics.UPDATE_INTERVAL.plusMillis( 1 ) );
    }
}
