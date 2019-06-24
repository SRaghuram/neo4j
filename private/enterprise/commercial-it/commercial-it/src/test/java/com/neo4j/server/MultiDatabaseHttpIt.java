/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server;

import com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings;
import com.neo4j.server.enterprise.CommercialNeoServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.HTTP;

import static com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings.mode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.server.http.cypher.integration.TransactionMatchers.hasErrors;
import static org.neo4j.server.rest.AbstractRestFunctionalTestBase.txCommitUri;
import static org.neo4j.test.server.HTTP.POST;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class MultiDatabaseHttpIt
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;
    private CommercialNeoServer commercialNeoServer;

    @AfterEach
    void tearDown()
    {
        if ( commercialNeoServer != null )
        {
            commercialNeoServer.stop();
        }
    }

    @Test
    void failToStartQueryExecutionOnFailedDatabase() throws IOException
    {
        String databaseName = "testDatabase";
        DatabaseLayout testDatabaseLayout = prepareEmptyDatabase( databaseName );

        fileSystem.deleteRecursively( testDatabaseLayout.getTransactionLogsDirectory() );

        commercialNeoServer = createService();
        HTTP.Response response = POST( txCommitUri( databaseName, httpPort() ), quotedJson( "{ 'statements': [ { 'statement': 'RETURN 1' } ] }" ) );
        assertThat( response, hasErrors( Status.General.DatabaseUnavailable ) );
    }

    private DatabaseLayout prepareEmptyDatabase( String databaseName )
    {
        commercialNeoServer = createService();
        var databaseService = commercialNeoServer.getDatabaseService();
        databaseService.getDatabaseManagementService().createDatabase( databaseName );
        var databaseApi = (GraphDatabaseAPI) databaseService.getDatabase( databaseName );
        DatabaseLayout testDatabaseLayout = databaseApi.databaseLayout();
        commercialNeoServer.stop();
        return testDatabaseLayout;
    }

    private int httpPort()
    {
        return commercialNeoServer.getWebServer().getLocalHttpAddress().getPort();
    }

    private CommercialNeoServer createService()
    {
        Config config = Config.newBuilder()
                .set( GraphDatabaseSettings.SERVER_DEFAULTS )
                .set( mode, CommercialEditionSettings.Mode.SINGLE.name() )
                .set( GraphDatabaseSettings.neo4j_home, testDirectory.storeDir().getAbsolutePath() )
                .set( GraphDatabaseSettings.auth_enabled, FALSE )
                .set( BoltConnector.group( "bolt" ).listen_address, "localhost:0" )
                .set( BoltConnector.group( "http" ).listen_address, "localhost:0" )
                .set( BoltConnector.group( "https" ).listen_address, "localhost:0" )
                .build();
        GraphDatabaseDependencies dependencies = GraphDatabaseDependencies.newDependencies().userLogProvider( NullLogProvider.getInstance() );
        CommercialNeoServer server = new CommercialNeoServer( config, dependencies );
        server.start();
        return server;
    }
}
