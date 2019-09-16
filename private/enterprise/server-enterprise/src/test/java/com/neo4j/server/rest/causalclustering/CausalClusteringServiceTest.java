/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import org.junit.jupiter.api.Test;

import java.net.URI;

import org.neo4j.configuration.Config;

import static com.neo4j.server.rest.causalclustering.CausalClusteringService.absoluteDatabaseClusterPath;
import static com.neo4j.server.rest.causalclustering.CausalClusteringService.databaseClusterUriPattern;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.server.configuration.ServerSettings.db_api_path;

class CausalClusteringServiceTest
{
    @Test
    void shouldReturnDatabaseManageUriPattern()
    {
        var config = Config.defaults( db_api_path, URI.create( "/custom/db" ) );

        var pattern = databaseClusterUriPattern( config );

        assertTrue( pattern.matcher( "/custom/db/neo4j/cluster" ).matches() );
        assertTrue( pattern.matcher( "/custom/db/foobar/cluster" ).matches() );
        assertTrue( pattern.matcher( "/custom/db/system/cluster" ).matches() );

        assertFalse( pattern.matcher( "/db/neo4j/cluster" ).matches() );
        assertFalse( pattern.matcher( "/custom/db/neo4j/causalclustering" ).matches() );
        assertFalse( pattern.matcher( "/custom/db/cluster" ).matches() );
        assertFalse( pattern.matcher( "/custom/db/neo4j/system/cluster" ).matches() );
    }

    @Test
    void shouldReturnAbsoluteDatabaseManagePath()
    {
        var config = Config.defaults( db_api_path, URI.create( "/foo/bar/db" ) );

        var path = absoluteDatabaseClusterPath( config );

        assertEquals( "/foo/bar/db/{databaseName}/cluster", path );
    }
}
