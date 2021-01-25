/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import org.junit.jupiter.api.Test;

import java.net.URI;

import org.neo4j.configuration.Config;

import static com.neo4j.server.rest.causalclustering.LegacyClusteringRedirectService.databaseLegacyClusterUriPattern;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.neo4j.server.configuration.ServerSettings.management_api_path;

class LegacyClusteringRedirectServiceTest
{
    @Test
    void shouldReturnDatabaseManageUriPattern()
    {
        var config = Config.defaults( management_api_path, URI.create( "/custom/db" ) );

        var pattern = databaseLegacyClusterUriPattern( config );

        assertThat( "/custom/db/server/causalclustering", matchesPattern( pattern ) );
        assertThat( "/custom/db/server/causalclustering", matchesPattern( pattern ) );
        assertThat( "/custom/db/server/causalclustering", matchesPattern( pattern ) );

        assertThat( "/custom/db/server/causalclustering/status", matchesPattern( pattern ) );
        assertThat( "/custom/db/server/causalclustering/available", matchesPattern( pattern ) );
        assertThat( "/custom/db/server/causalclustering/writable", matchesPattern( pattern ) );
        assertThat( "/custom/db/server/causalclustering/read-only", matchesPattern( pattern ) );

        assertThat( "/db/neo4j/manage/causalclustering", not( matchesPattern( pattern ) ) );
        assertThat( "/custom/db/neo4j/causalclustering", not( matchesPattern( pattern ) ) );
        assertThat( "/custom/db/manage/causalclustering", not( matchesPattern( pattern ) ) );
        assertThat( "/custom/db/neo4j/system/manage/causalclustering", not( matchesPattern( pattern ) ) );
    }
}
