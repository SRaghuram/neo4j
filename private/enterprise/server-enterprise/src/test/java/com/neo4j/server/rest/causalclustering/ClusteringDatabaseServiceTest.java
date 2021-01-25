/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import org.junit.jupiter.api.Test;

import java.net.URI;

import org.neo4j.configuration.Config;

import static com.neo4j.server.rest.causalclustering.ClusteringDatabaseService.absoluteDatabaseClusterPath;
import static com.neo4j.server.rest.causalclustering.ClusteringDatabaseService.databaseClusterUriPattern;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.server.configuration.ServerSettings.db_api_path;

class ClusteringDatabaseServiceTest
{
    @Test
    void shouldReturnDatabaseManageUriPattern()
    {
        var config = Config.defaults( db_api_path, URI.create( "/custom/db" ) );

        var pattern = databaseClusterUriPattern( config );

        assertThat( "/custom/db/neo4j/cluster", matchesPattern( pattern ) );
        assertThat( "/custom/db/foobar/cluster", matchesPattern( pattern ) );
        assertThat( "/custom/db/system/cluster", matchesPattern( pattern ) );

        assertThat( "/custom/db/system/cluster/status", matchesPattern( pattern ) );
        assertThat( "/custom/db/system/cluster/available", matchesPattern( pattern ) );
        assertThat( "/custom/db/foobar/cluster/writable", matchesPattern( pattern ) );
        assertThat( "/custom/db/neo4j/cluster/read-only", matchesPattern( pattern ) );

        assertThat( "/db/neo4j/cluster", not( matchesPattern( pattern ) ) );
        assertThat( "/custom/db/neo4j/causalclustering", not( matchesPattern( pattern ) ) );
        assertThat( "/custom/db/cluster", not( matchesPattern( pattern ) ) );
        assertThat( "/custom/db/neo4j/system/cluster", not( matchesPattern( pattern ) ) );
    }

    @Test
    void shouldReturnAbsoluteDatabaseManagePath()
    {
        var config = Config.defaults( db_api_path, URI.create( "/foo/bar/db" ) );

        var path = absoluteDatabaseClusterPath( config );

        assertEquals( "/foo/bar/db/{databaseName}/cluster", path );
    }
}
