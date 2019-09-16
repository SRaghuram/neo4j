/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import org.junit.jupiter.api.Test;

import java.net.URI;

import org.neo4j.configuration.Config;

import static com.neo4j.server.rest.causalclustering.LegacyCausalClusteringRedirectService.databaseLegacyClusterUriPattern;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.server.configuration.ServerSettings.management_api_path;

class LegacyCausalClusteringRedirectServiceTest
{
    @Test
    void shouldReturnDatabaseManageUriPattern()
    {
        var config = Config.defaults( management_api_path, URI.create( "/custom/db" ) );

        var pattern = databaseLegacyClusterUriPattern( config );

        assertTrue( pattern.matcher( "/custom/db/server/causalclustering" ).matches() );
        assertTrue( pattern.matcher( "/custom/db/server/causalclustering" ).matches() );
        assertTrue( pattern.matcher( "/custom/db/server/causalclustering" ).matches() );

        assertFalse( pattern.matcher( "/db/neo4j/manage/causalclustering" ).matches() );
        assertFalse( pattern.matcher( "/custom/db/neo4j/causalclustering" ).matches() );
        assertFalse( pattern.matcher( "/custom/db/manage/causalclustering" ).matches() );
        assertFalse( pattern.matcher( "/custom/db/neo4j/system/manage/causalclustering" ).matches() );
    }
}
