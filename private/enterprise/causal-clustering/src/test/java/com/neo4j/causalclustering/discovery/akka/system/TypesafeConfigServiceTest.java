/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import com.neo4j.causalclustering.discovery.TestFirstStartupDetector;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.logging.Level;

import static com.neo4j.causalclustering.discovery.akka.system.TypesafeConfigService.ArteryTransport.TCP;
import static com.neo4j.configuration.CausalClusteringSettings.discovery_advertised_address;
import static com.neo4j.configuration.CausalClusteringSettings.discovery_listen_address;
import static com.neo4j.configuration.CausalClusteringSettings.middleware_logging_level;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TypesafeConfigServiceTest
{
    @ParameterizedTest
    @EnumSource( AkkaLoggingLevel.class )
    void shouldUseLogLevel( AkkaLoggingLevel akkaLevel )
    {
        var neo4jConfig = neo4jConfigWithMiddlewareLoggingLevel( akkaLevel.neo4jLevel() );
        var akkaConfigService = new TypesafeConfigService( TCP, new TestFirstStartupDetector( true ), neo4jConfig );

        var akkaConfig = akkaConfigService.generate();

        assertEquals( akkaLevel.toString(), akkaConfig.getString( "akka.loglevel" ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"localhost.com", "LOCALHOST.COM", "LocalHost.Com"} )
    void akkaAddressShouldBeAlwaysLowerCased( String hostname )
    {
        var neo4jConfig = neo4jConfigWithHost( hostname );
        var akkaConfigService = new TypesafeConfigService( TCP, new TestFirstStartupDetector( true ), neo4jConfig );

        var akkaConfig = akkaConfigService.generate();

        assertEquals( hostname.toLowerCase(), akkaConfig.getString( "akka.remote.artery.canonical.hostname" ) );
        assertEquals( hostname.toLowerCase(), akkaConfig.getString( "akka.remote.artery.bind.hostname" ) );
    }

    private static Config neo4jConfigWithMiddlewareLoggingLevel( Level level )
    {
        return Config.defaults( middleware_logging_level, level );
    }

    private static Config neo4jConfigWithHost( String hostname )
    {
        var socketAddress = new SocketAddress( hostname, 5000 );
        return Config.defaults( Map.of( discovery_advertised_address, socketAddress, discovery_listen_address, socketAddress ) );
    }
}
