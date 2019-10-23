/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.server.enterprise.helpers.EnterpriseServerBuilder;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.server.NeoServer;
import org.neo4j.server.NeoServerRestartTestIT;
import org.neo4j.server.database.DatabaseService;
import org.neo4j.server.helpers.CommunityServerBuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class NeoServerRestartTestEnterpriseIT extends NeoServerRestartTestIT
{
    @Override
    protected NeoServer getNeoServer( String customPageSwapperName ) throws IOException
    {
        CommunityServerBuilder builder = EnterpriseServerBuilder.serverOnRandomPorts()
                                                                .persistent()
                                                                .usingDataDir( folder.homeDir().getAbsolutePath() )
                                                                .withProperty( GraphDatabaseSettings.pagecache_swapper.name(), customPageSwapperName );
        return builder.build();
    }

    @Test
    public void restartWithDefaultDbStopped() throws Exception
    {
        NeoServer server = getNeoServer( CUSTOM_SWAPPER );
        server.start();
        server.getDatabaseService().getDatabaseManagementService().shutdownDatabase( DEFAULT_DATABASE_NAME );
        server.stop();

        server = getNeoServer( CUSTOM_SWAPPER );
        server.start();
        DatabaseService databaseService = server.getDatabaseService();
        assertThat( databaseService.getSystemDatabase().isAvailable( 0 ), equalTo( true ) );
        assertThat( databaseService.getDatabase( DEFAULT_DATABASE_NAME ).isAvailable( 0 ), equalTo( false ) );
        server.stop();
    }
}
