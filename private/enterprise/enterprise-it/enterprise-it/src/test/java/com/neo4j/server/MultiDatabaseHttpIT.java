/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server;

import com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import com.neo4j.server.enterprise.EnterpriseNeoServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.HTTP;

import static com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings.mode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.server.http.cypher.integration.TransactionMatchers.hasErrors;
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
    private EnterpriseNeoServer enterpriseNeoServer;

    @AfterEach
    void tearDown()
    {
        if ( enterpriseNeoServer != null )
        {
            enterpriseNeoServer.stop();
        }
    }

    @Test
    void failToStartQueryExecutionOnFailedDatabase() throws IOException
    {
        String databaseName = "testDatabase";
        DatabaseLayout testDatabaseLayout = prepareEmptyDatabase( databaseName );

        fileSystem.deleteRecursively( testDatabaseLayout.getTransactionLogsDirectory() );

        enterpriseNeoServer = createService();
        HTTP.Response response = POST( txCommitUri( databaseName, httpPort() ), quotedJson( "{ 'statements': [ { 'statement': 'RETURN 1' } ] }" ) );
        assertThat( response, hasErrors( Status.General.DatabaseUnavailable ) );
    }

    private DatabaseLayout prepareEmptyDatabase( String databaseName )
    {
        enterpriseNeoServer = createService();
        var databaseService = enterpriseNeoServer.getDatabaseService();
        databaseService.getDatabaseManagementService().createDatabase( databaseName );
        var databaseApi = (GraphDatabaseAPI) databaseService.getDatabase( databaseName );
        DatabaseLayout testDatabaseLayout = databaseApi.databaseLayout();
        enterpriseNeoServer.stop();
        return testDatabaseLayout;
    }

    private int httpPort()
    {
        return enterpriseNeoServer.getWebServer().getLocalHttpAddress().getPort();
    }

    private EnterpriseNeoServer createService()
    {
        Config config = Config.newBuilder()
                .setDefaults( GraphDatabaseSettings.SERVER_DEFAULTS )
                .set( mode, EnterpriseEditionSettings.Mode.SINGLE )
                .set( GraphDatabaseSettings.neo4j_home, testDirectory.homeDir().toPath().toAbsolutePath() )
                .set( GraphDatabaseSettings.auth_enabled, false )
                .set( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) )
                .set( HttpConnector.listen_address, new SocketAddress( "localhost", 0 ) )
                .set( HttpsConnector.listen_address, new SocketAddress( "localhost", 0 ) )
                .build();
        GraphDatabaseDependencies dependencies = GraphDatabaseDependencies.newDependencies().userLogProvider( NullLogProvider.getInstance() );
        EnterpriseNeoServer server = new EnterpriseNeoServer( config, dependencies );
        server.start();
        return server;
    }
}
