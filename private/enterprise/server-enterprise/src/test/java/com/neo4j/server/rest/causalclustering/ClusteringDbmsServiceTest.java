/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;

class ClusteringDbmsServiceTest
{
    @Test
    void shouldReturnDbmsClusterEndpointPattern()
    {
        var pattern = ClusteringDbmsService.dbmsClusterUriPattern();

        assertThat( "/dbms/cluster", matchesPattern( pattern ) );
        assertThat( "/dbms/cluster/", matchesPattern( pattern ) );
        assertThat( "/dbms/cluster/status", matchesPattern( pattern ) );
        assertThat( "/dbms/cluster/status/", matchesPattern( pattern ) );
        assertThat( "/dbms/cluster/foo/bar/baz/qux", matchesPattern( pattern ) );
        assertThat( "/dbms/cluster/foo/bar/baz/qux/", matchesPattern( pattern ) );

        assertThat( "/db/cluster", not( matchesPattern( pattern ) ) );
        assertThat( "/dbms/foo", not( matchesPattern( pattern ) ) );
        assertThat( "/dbms/dbms/cluster", not( matchesPattern( pattern ) ) );
        assertThat( "/dbms/dbms/cluster", not( matchesPattern( pattern ) ) );
    }
}
