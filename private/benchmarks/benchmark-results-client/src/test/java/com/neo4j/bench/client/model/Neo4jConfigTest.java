/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.model;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class Neo4jConfigTest
{
    @Test
    public void shouldSerializeToJson()
    {
        Neo4jConfig defaults = Neo4jConfig.withDefaults();

        String json = defaults.toJson();
        Neo4jConfig config = Neo4jConfig.fromJson( json );

        assertThat( config, is( defaults ) );
    }
}
