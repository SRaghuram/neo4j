/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.config;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.NormalizedGraphName;
import org.neo4j.configuration.helpers.SocketAddress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
        assertEquals( "mega", database.getName().name() );
        assertEquals( Set.of(
                new FabricConfig.Graph( 0L, FabricConfig.RemoteUri.create( "bolt://mega:1111" ), null, null, emptyDriverConfig() ),
                new FabricConfig.Graph( 1L, FabricConfig.RemoteUri.create( "bolt://mega:2222" ), "db0", new NormalizedGraphName( "source-of-all-wisdom" ),
                        emptyDriverConfig() )
        ), database.getGraphs() );
    }

    @Test
    void testDatabaseNameNormalization()
    {
        var properties = Map.of(
                "fabric.database.name", "MeGa"
        );

        var config = Config.newBuilder()
                .setRaw( properties )
                .build();

        var fabricConfig = FabricConfig.from( config );
        assertTrue( fabricConfig.isEnabled() );

        var database = fabricConfig.getDatabase();
        assertEquals( "mega", database.getName().name() );
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
    void testDuplicateGraphNames()
    {
        var properties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", "bolt://localhost:7687",
                "fabric.graph.1.uri", "bolt://localhost:7687",
                "fabric.graph.0.name", "foo",
                "fabric.graph.1.name", "foo"
        );

        var config = Config.newBuilder()
                .setRaw( properties )
                .build();

        var e = assertThrows( IllegalArgumentException.class,
                () -> FabricConfig.from( config ) );

        assertEquals( e.getMessage(), "Graphs with ids: 0, 1, have conflicting names" );
    }

    @Test
    void testDuplicateGraphNamesNormalized()
    {
        var properties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.3.uri", "bolt://localhost:7687",
                "fabric.graph.0.uri", "bolt://localhost:7687",
                "fabric.graph.1.uri", "bolt://localhost:7687",
                "fabric.graph.2.uri", "bolt://localhost:7687",
                "fabric.graph.1.name", "Foo",
                "fabric.graph.2.name", "bar",
                "fabric.graph.3.name", "FOO",
                "fabric.graph.0.name", "foo"

        );

        var config = Config.newBuilder()
                .setRaw( properties )
                .build();

        var e = assertThrows( IllegalArgumentException.class,
                () -> FabricConfig.from( config ) );

        assertEquals( e.getMessage(), "Graphs with ids: 0, 1, 3, have conflicting names" );
    }

    @Test
    void testInvalidDatabaseName()
    {
        var properties = Map.of(
                "fabric.database.name", "mega!"
        );

        assertThrows( IllegalArgumentException.class,
                () -> Config.newBuilder()
                        .setRaw( properties )
                        .build()
        );
    }

    @Test
    void testInvalidGraphName()
    {
        var properties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", "bolt://localhost:7687",
                "fabric.graph.0.name", "foo!"
        );

        assertThrows( IllegalArgumentException.class,
                () -> Config.newBuilder()
                        .setRaw( properties )
                        .build()
        );
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
    void testEmptyConfig()
    {
        var properties = Map.<String,String>of();

        var config = Config.newBuilder()
                .setRaw( properties )
                .build();

        var fabricConfig = FabricConfig.from( config );
        assertFalse( fabricConfig.isEnabled() );
        assertNull( fabricConfig.getDatabase() );
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

        assertThrows( IllegalArgumentException.class, () -> FabricConfig.from( config ) );
    }

    @Test
    void testRemoteUriList()
    {
        var fabricConfig = doTestRemoteUri( "bolt://core-1:1111?key=value,bolt://core-2:2222?key=value" );
        var uri = fabricConfig.getDatabase().getGraphs().stream().findFirst().get().getUri();
        assertEquals( uri.getScheme(), "bolt" );
        assertEquals( uri.getQuery(), "key=value" );
        assertThat( uri.getAddresses() ).contains( new SocketAddress( "core-1", 1111 ), new SocketAddress( "core-2", 2222 ) );
    }

    @Test
    void testInvalidRemoteUriList()
    {
        assertThrows( IllegalArgumentException.class, () -> doTestRemoteUri( "neo4j://core-1:1111,bolt://core-2:2222" ) );
        assertThrows( IllegalArgumentException.class, () -> doTestRemoteUri( "neo4j://core-1:1111?key=value,bolt://core-2:2222" ) );
        assertThrows( IllegalArgumentException.class, () -> doTestRemoteUri( "" ) );
        assertThrows( IllegalArgumentException.class, () -> doTestRemoteUri( "neo4j://core-1" ) );
        assertThrows( IllegalArgumentException.class, () -> doTestRemoteUri( "neo4j://" ) );
    }

    private FabricConfig doTestRemoteUri( String uri )
    {
        var properties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", uri
        );

        var config = Config.newBuilder()
                .setRaw( properties )
                .build();

        return FabricConfig.from( config );
    }

    private FabricConfig.GraphDriverConfig emptyDriverConfig()
    {
        return new FabricConfig.GraphDriverConfig( null, null, null, null, null, null, null, null, true );
    }
}
