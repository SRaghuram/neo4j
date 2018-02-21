/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.server.enterprise.helpers.CommercialServerBuilder;

import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.server.NeoServer;
import org.neo4j.server.NeoServerRestartTestIT;
import org.neo4j.server.helpers.CommunityServerBuilder;

public class NeoServerRestartTestEnterpriseIT extends NeoServerRestartTestIT
{
    protected NeoServer getNeoServer( String customPageSwapperName ) throws IOException
    {
        CommunityServerBuilder builder = CommercialServerBuilder.serverOnRandomPorts()
                .withProperty( GraphDatabaseSettings.pagecache_swapper.name(), customPageSwapperName );
        return builder.build();
    }
}
