/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.config;

import com.neo4j.configuration.FabricEnterpriseConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.NormalizedGraphName;
import org.neo4j.configuration.helpers.SocketAddress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

        var fabricConfig = toFabricEnterpriseConfig( properties );

        var database = fabricConfig.getDatabase();
        assertEquals( "mega", database.getName().name() );
        assertEquals( Set.of(
                new FabricEnterpriseConfig.Graph( 0L, FabricEnterpriseConfig.RemoteUri.create( "bolt://mega:1111" ), null, null, emptyDriverConfig() ),
                new FabricEnterpriseConfig.Graph( 1L, FabricEnterpriseConfig.RemoteUri.create( "bolt://mega:2222" ), "db0",
                        new NormalizedGraphName( "source-of-all-wisdom" ), emptyDriverConfig() )
        ), database.getGraphs() );
    }

    @Test
    void testDatabaseNameNormalization()
    {
        var properties = Map.of(
                "fabric.database.name", "MeGa"
        );

        var fabricConfig = toFabricEnterpriseConfig( properties );

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
                () -> FabricEnterpriseConfig.from( config ) );
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
                () -> FabricEnterpriseConfig.from( config ) );

        assertEquals( "Graphs with ids: 0, 1, have conflicting names", e.getMessage() );
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
                () -> FabricEnterpriseConfig.from( config ) );

        assertEquals( "Graphs with ids: 0, 1, 3, have conflicting names", e.getMessage() );
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

        var fabricConfig = toFabricEnterpriseConfig( properties );
        assertNull( fabricConfig.getDatabase() );
    }

    @Test
    void testEmptyConfig()
    {
        var properties = Map.<String,String>of();

        var fabricConfig = toFabricEnterpriseConfig( properties );
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

        assertThrows( IllegalArgumentException.class, () -> FabricEnterpriseConfig.from( config ) );
    }

    @Test
    void testRemoteUriList()
    {
        var fabricConfig = doTestRemoteUri( "bolt://core-1:1111?key=value,bolt://core-2:2222?key=value" );
        var uri = fabricConfig.getDatabase().getGraphs().stream().findFirst().get().getUri();
        assertEquals( "bolt", uri.getScheme() );
        assertEquals( "key=value", uri.getQuery() );
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

    @Test
    void testBufferSizeConstraint()
    {
        doTestStreamConstraint( "fabric.stream.buffer.size", "0",
                "Failed to validate '0' for 'fabric.stream.buffer.size': minimum allowed value is 1" );
    }

    @Test
    void testBufferLowWatermarkConstraint()
    {
        doTestStreamConstraint( "fabric.stream.buffer.low_watermark", "-1",
                "Failed to validate '-1' for 'fabric.stream.buffer.low_watermark': minimum allowed value is 0" );
    }

    @Test
    void testBatchSizeConstraint()
    {
        doTestStreamConstraint( "fabric.stream.batch_size", "0",
                "Failed to validate '0' for 'fabric.stream.batch_size': minimum allowed value is 1" );
    }

    @Test
    void testConcurrencyConstraint()
    {
        doTestStreamConstraint( "fabric.stream.concurrency", "0",
                "Failed to validate '0' for 'fabric.stream.concurrency': minimum allowed value is 1" );
    }

    @Test
    void testLowWatermarkBiggerThanBufferSize()
    {
        var properties = Map.of(
                "fabric.database.name",
                "mega", "fabric.graph.0.uri",
                "bolt://mega:1111",
                "fabric.stream.buffer.size", "10",
                "fabric.stream.buffer.low_watermark'", "20"
        );

        var fabricConfig = toFabricEnterpriseConfig( properties );
        assertEquals( 10, fabricConfig.getDataStream().getBufferLowWatermark() );
    }

    @Test
    void testDefaultRoutingServers()
    {
        var properties = Map.of( "fabric.database.name", "mega" );

        var fabricConfig = toFabricEnterpriseConfig( properties );
        assertThat( fabricConfig.getFabricServers() ).contains( new SocketAddress( "localhost", 7687 ) );
    }

    @Test
    void testRoutingServersDelegatedToDefaultAdvertisedAddress()
    {
        var properties = Map.of(
                "fabric.database.name", "mega",
                "dbms.default_listen_address", "somewhere",
                "dbms.default_advertised_address", "somewhere-else"
        );

        var fabricConfig = toFabricEnterpriseConfig( properties );
        assertThat( fabricConfig.getFabricServers() ).contains( new SocketAddress( "somewhere-else", 7687 ) );
    }

    @Test
    void testRoutingServersDelegatedToBoltAdvertisedAddress()
    {
        var properties = Map.of(
                "fabric.database.name", "mega",
                "dbms.connector.bolt.listen_address", "somewhere:6543",
                "dbms.connector.bolt.advertised_address", "somewhere-else"
        );

        var fabricConfig = toFabricEnterpriseConfig( properties );
        assertThat( fabricConfig.getFabricServers() ).contains( new SocketAddress( "somewhere-else", 6543 ) );
    }

    @Test
    void testRoutingServersConfigured()
    {
        var properties = Map.of(
                "fabric.database.name", "mega",
                "fabric.routing.servers", "host-1:1111,host-2:2222"
        );

        var fabricConfig = toFabricEnterpriseConfig( properties );
        assertThat( fabricConfig.getFabricServers() ).contains( new SocketAddress( "host-1", 1111 ), new SocketAddress( "host-2", 2222 ) );
    }

    private FabricEnterpriseConfig toFabricEnterpriseConfig( Map<String,String> properties )
    {
        var config = Config.newBuilder().setRaw( properties ).build();
        return FabricEnterpriseConfig.from( config );
    }

    void doTestStreamConstraint( String settingKey, String settingValue, String expectedMessage )
    {
        var properties = Map.of(
                "fabric.database.name",
                "mega", "fabric.graph.0.uri",
                "bolt://mega:1111",
                settingKey, settingValue
        );

        var thrown = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder().setRaw( properties ).build() );

        assertThat( thrown.getMessage() ).contains( expectedMessage );
    }

    private FabricEnterpriseConfig doTestRemoteUri( String uri )
    {
        var properties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", uri
        );

        var config = Config.newBuilder()
                .setRaw( properties )
                .build();

        return FabricEnterpriseConfig.from( config );
    }

    private FabricEnterpriseConfig.GraphDriverConfig emptyDriverConfig()
    {
        return new FabricEnterpriseConfig.GraphDriverConfig( null, null, null, null, null, null, null, null, true );
    }
}
