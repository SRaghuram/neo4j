/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.config;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.Config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FabricConfigTest
{

    @Test
    void testLoadConfig()
    {
        var properties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", "bolt://mega:1111",
                "fabric.graph.1.uri", "bolt://mega:2222",
                "fabric.graph.1.database", "db0",
                "fabric.graph.1.name", "source-of-all-wisdom"
        );

        var config = Config.newBuilder()
                .setRaw( properties )
                .build();

        var fabricConfig = FabricConfig.from( config );
        assertTrue( fabricConfig.isEnabled() );

        var database = fabricConfig.getDatabase();
        assertEquals( "mega", database.getName() );
        assertEquals( Set.of(
                new FabricConfig.Graph( 0L, URI.create( "bolt://mega:1111" ), "neo4j", null ),
                new FabricConfig.Graph( 1L, URI.create( "bolt://mega:2222" ), "db0", "source-of-all-wisdom" )
                ), database.getGraphs() );
    }

    @Test
    void testLoadInvalidConfig()
    {
        var properties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", "bolt://localhost:7687",
                "fabric.graph.foo.uri", "bolt://localhost:7687"
        );

        var config = Config.newBuilder()
                .setRaw( properties )
                .build();

        assertThrows( IllegalArgumentException.class,
                () -> FabricConfig.from( config ) );
    }

    @Test
    void testNoFabricDb()
    {
        var properties = Map.of(
                "fabric.graph.0.uri", "bolt://mega:1111",
                "fabric.graph.1.uri", "bolt://mega:2222"
        );

        var config = Config.newBuilder()
                .setRaw( properties )
                .build();

        var fabricConfig = FabricConfig.from( config );
        assertFalse( fabricConfig.isEnabled() );
    }

    @Test
    void testRequired()
    {
        var properties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.1.database", "db0",
                "fabric.graph.1.name", "source-of-all-wisdom"
        );

        var config = Config.newBuilder()
                .setRaw( properties )
                .build();

        assertThrows( IllegalArgumentException.class, () -> FabricConfig.from( config ));
    }
}
