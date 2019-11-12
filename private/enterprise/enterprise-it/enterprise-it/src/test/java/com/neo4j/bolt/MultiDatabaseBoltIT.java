/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bolt.BoltDriverHelper.graphDatabaseDriver;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.driver.SessionConfig.forDatabase;

@TestDirectoryExtension
class MultiDatabaseBoltIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;
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
    void failToStartQueryExecutionOnFailedDatabase() throws IOException
    {
        String databaseName = "testDatabase";
        DatabaseLayout testDatabaseLayout = prepareEmptyDatabase( databaseName );

        fileSystem.deleteRecursively( testDatabaseLayout.getTransactionLogsDirectory() );

        managementService = createManagementService();
        TransientException transientException = assertThrows( TransientException.class, () ->
        {
            try ( var driver = graphDatabaseDriver( boltAddress() );
                  var session = driver.session( forDatabase( databaseName ) ) )
            {
                session.run( "CREATE (n)" ).consume();
            }
        } );
        assertThat( transientException.getMessage(), equalTo( "Database `testDatabase` is unavailable." ) );
    }

    private DatabaseLayout prepareEmptyDatabase( String databaseName )
    {
        managementService = createManagementService();
        managementService.createDatabase( databaseName );
        var databaseApi = (GraphDatabaseAPI) managementService.database( databaseName );
        DatabaseLayout testDatabaseLayout = databaseApi.databaseLayout();
        managementService.shutdown();
        return testDatabaseLayout;
    }

    private String boltAddress()
    {
        var db = (GraphDatabaseAPI) managementService.database( SYSTEM_DATABASE_NAME );
        ConnectorPortRegister portRegister = db.getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
        return "bolt://" + portRegister.getLocalAddress( "bolt" );
    }

    private DatabaseManagementService createManagementService()
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                .setConfig( BoltConnector.enabled, true )
                .setConfig( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) )
                .build();
    }
}
