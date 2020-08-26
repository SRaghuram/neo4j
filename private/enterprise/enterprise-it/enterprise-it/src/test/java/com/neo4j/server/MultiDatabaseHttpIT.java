/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server;

import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.HTTP;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.mode;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.hasErrors;
import static org.neo4j.server.rest.AbstractRestFunctionalTestBase.txCommitUri;
import static org.neo4j.test.server.HTTP.POST;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

@TestDirectoryExtension
class MultiDatabaseHttpIT
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

        managementService = createService();
        HTTP.Response response = POST( txCommitUri( databaseName, httpPort() ), quotedJson( "{ 'statements': [ { 'statement': 'RETURN 1' } ] }" ) );
        assertThat( response ).satisfies( hasErrors( Status.Database.DatabaseUnavailable ) );
    }

    private DatabaseLayout prepareEmptyDatabase( String databaseName )
    {
        managementService = createService();
        managementService.createDatabase( databaseName );
        var databaseApi = (GraphDatabaseAPI) managementService.database( databaseName );
        DatabaseLayout testDatabaseLayout = databaseApi.databaseLayout();
        managementService.shutdown();
        return testDatabaseLayout;
    }

    private int httpPort()
    {
        var databaseAPI = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        var portRegister = databaseAPI.getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
        return portRegister.getLocalAddress( "http" ).getPort();
    }

    private DatabaseManagementService createService()
    {
        Config custom = Config.newBuilder()
                .setDefaults( GraphDatabaseSettings.SERVER_DEFAULTS )
                .set( mode, GraphDatabaseSettings.Mode.SINGLE )
                .set( GraphDatabaseSettings.neo4j_home, testDirectory.homePath().toAbsolutePath() )
                .set( GraphDatabaseSettings.auth_enabled, false )
                .set( OnlineBackupSettings.online_backup_enabled, false )
                .set( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) )
                .set( HttpConnector.listen_address, new SocketAddress( "localhost", 0 ) )
                .build();
        return new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() ).setConfig( custom )
                .setUserLogProvider( NullLogProvider.getInstance() ).build();
    }
}
