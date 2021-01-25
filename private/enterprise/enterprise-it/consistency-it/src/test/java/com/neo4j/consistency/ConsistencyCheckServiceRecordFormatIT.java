/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.consistency;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.assertj.core.description.Description;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.configuration.Config;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.store.format.aligned.PageAlignedV4_1;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static org.neo4j.internal.helpers.progress.ProgressMonitorFactory.NONE;

@Neo4jLayoutExtension
class ConsistencyCheckServiceRecordFormatIT
{
    @Inject
    private DatabaseLayout databaseLayout;

    @ParameterizedTest
    @ValueSource( strings = {Standard.LATEST_NAME, HighLimit.NAME, PageAlignedV4_1.NAME} )
    void checkTinyConsistentStore( String recordFormat ) throws Exception
    {
        var managementService = new TestEnterpriseDatabaseManagementServiceBuilder( databaseLayout )
                .setConfig( record_format, recordFormat ).build();
        try
        {
            var database = managementService.database( DEFAULT_DATABASE_NAME );
            createTestData( database );
        }
        finally
        {
            managementService.shutdown();
        }
        assertConsistentStore( databaseLayout );
    }

    private static void createTestData( GraphDatabaseService db )
    {
        Node previous = null;
        var robot = Label.label( "robot" );
        var human = Label.label( "human" );
        var create = RelationshipType.withName( "create" );
        var destroy = RelationshipType.withName( "destroy" );
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 1000; i++ )
            {
                Label label = (i % 2 == 0) ? robot : human;
                Node current = tx.createNode( label );
                current.setProperty( "value", ThreadLocalRandom.current().nextLong() );

                if ( previous != null )
                {
                    previous.createRelationshipTo( current, create );
                    current.createRelationshipTo( previous, destroy );
                }
                previous = current;
            }
            tx.commit();
        }
    }

    private static void assertConsistentStore( DatabaseLayout databaseLayout ) throws Exception
    {
        ConsistencyCheckService service = new ConsistencyCheckService();
        AssertableLogProvider logProvider = new AssertableLogProvider();
        ConsistencyCheckService.Result result = service.runFullConsistencyCheck( databaseLayout, Config.defaults(), NONE, logProvider, false );
        assertThat( result.isSuccessful() ).as( new Description()
        {
            @Override
            public String value()
            {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                try ( PrintStream stream = new PrintStream( out ) )
                {
                    logProvider.print( stream );
                }
                return out.toString();
            }
        } ).isTrue();
    }
}
