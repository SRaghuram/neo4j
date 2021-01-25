/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readonly;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.ReadOnlyDbException;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_database_default;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_databases;

@TestDirectoryExtension
public class ReadOnlyStandaloneIT
{
    @Inject
    TestDirectory testDirectory;
    private DatabaseManagementService managementService;

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void shouldFailToWriteTxIfGlobalReadOnlyIsOn()
    {
        //given
        DatabaseManagementServiceBuilder builder = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() );
        managementService = builder.setConfig( Map.of( read_only_database_default, true ) ).build();
        final GraphDatabaseService service = managementService.database( DEFAULT_DATABASE_NAME );
        final var property = "property";
        final var label = "test";

        //when/then
        var t = assertThrows( RuntimeException.class, () -> createData( service, property, label ) );
        assertThat( t ).hasCauseInstanceOf( ReadOnlyDbException.class );
        managementService.shutdown();
    }

    @Test
    void shouldFailToWriteTxIfDBIsReadOnly()
    {
        //given
        DatabaseManagementServiceBuilder builder = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() );
        managementService = builder.setConfig( Map.of( read_only_database_default, false,
                                                       read_only_databases, Set.of( DEFAULT_DATABASE_NAME ) ) )
                                                                   .build();
        final GraphDatabaseService service = managementService.database( DEFAULT_DATABASE_NAME );
        final var property = "property";
        final var label = "test";

        //when/then
        var t = assertThrows( RuntimeException.class, () -> createData( service, property, label ) );
        assertThat( t ).hasCauseInstanceOf( ReadOnlyDbException.class );
        managementService.shutdown();
    }

    @Test
    void shouldSuccessfullyWriteTxIfDBIsNotReadOnly()
    {
        //given
        DatabaseManagementServiceBuilder builder = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() );
        managementService = builder.build();
        final GraphDatabaseService service = managementService.database( DEFAULT_DATABASE_NAME );
        final var property = "property";
        final var label = "test";

        //when
        createData( service, property, label );

        //then
        final var propertyExists = dataExists( (GraphDatabaseFacade) service, tx ->
        {
            final var iterator = tx.getAllNodes().iterator();
            assertThat( iterator.next().getPropertyKeys().iterator().next() ).isEqualTo( property );
            iterator.close();
        } );
        final var labelExists = dataExists( (GraphDatabaseFacade) service, tx ->
                assertThat( tx.getAllLabels().iterator().next().name() ).isEqualTo( label ) );

        assertTrue( propertyExists );
        assertTrue( labelExists );
        managementService.shutdown();
    }

    private void createData( GraphDatabaseService service, String property, String label )
    {
        try ( Transaction tx = service.beginTx() )
        {
            tx.execute( "CREATE (:test {" + property + ":\"" + label + "\"})" );
            tx.commit();
        }
    }

    private boolean dataExists( GraphDatabaseFacade facade, Consumer<Transaction> validationFunc )
    {
        try ( var tx = facade.beginTx() )
        {
            try
            {
                validationFunc.accept( tx );
                return true;
            }
            catch ( Exception ignored )
            {
            }
        }
        return false;
    }
}
