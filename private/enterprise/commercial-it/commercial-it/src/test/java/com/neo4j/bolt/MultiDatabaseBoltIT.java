/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import com.neo4j.commercial.edition.factory.CommercialDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.TRUE;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
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
            try ( var driver = GraphDatabase.driver( boltAddress() ); var session = driver.session( template -> template.withDatabase( databaseName ) ) )
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
        return new CommercialDatabaseManagementServiceBuilder( testDirectory.storeDir() )
                .setConfig( BoltConnector.group( "bolt" ).enabled, TRUE )
                .setConfig( BoltConnector.group( "bolt" ).listen_address, "localhost:0" )
                .build();
    }
}
