/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.neo4j.configuration.Config;
import org.neo4j.logging.Level;

import static com.neo4j.causalclustering.discovery.akka.system.TypesafeConfigService.ArteryTransport.TCP;
import static com.neo4j.configuration.CausalClusteringSettings.middleware_logging_level;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TypesafeConfigServiceTest
{
    @ParameterizedTest
    @EnumSource( AkkaLoggingLevel.class )
    void shouldUseLogLevel( AkkaLoggingLevel akkaLevel )
    {
        var neo4jConfig = neo4jConfigWithMiddlewareLoggingLevel( akkaLevel.neo4jLevel() );
        var akkaConfigService = new TypesafeConfigService( TCP, neo4jConfig );

        var akkaConfig = akkaConfigService.generate();

        assertEquals( akkaLevel.toString(), akkaConfig.getString( "akka.loglevel" ) );
    }

    private static Config neo4jConfigWithMiddlewareLoggingLevel( Level level )
    {
        return Config.defaults( middleware_logging_level, level );
    }
}
