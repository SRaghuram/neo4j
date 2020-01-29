/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import com.neo4j.causalclustering.common.Cluster;

import java.net.URI;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.net.ServerAddress;
import org.neo4j.driver.net.ServerAddressResolver;

import static java.util.stream.Collectors.toSet;

public class BoltDriverHelper
{
    private static Config.ConfigBuilder baseConfig()
    {
        return Config.builder().withoutEncryption().withLogging( Logging.none() );
    }

    public static Driver graphDatabaseDriver( URI uri )
    {
        return GraphDatabase.driver( uri, baseConfig().build() );
    }

    public static Driver graphDatabaseDriver( String uri )
    {
        return GraphDatabase.driver( uri, baseConfig().build() );
    }

    public static Driver graphDatabaseDriver( URI uri, AuthToken auth )
    {
        return GraphDatabase.driver( uri, auth, baseConfig().build() );
    }

    public static Driver graphDatabaseDriver( String uri, AuthToken auth )
    {
        return GraphDatabase.driver( uri, auth, baseConfig().build() );
    }

    public static Driver graphDatabaseDriver( Cluster cluster, AuthToken auth )
    {
        ServerAddressResolver serverAddressResolver = address -> cluster
                .coreMembers()
                .stream()
                .map( c -> URI.create( c.routingURI() ) )
                .map( uri -> ServerAddress.of( uri.getHost(), uri.getPort() ) )
                .collect( toSet() );

        return GraphDatabase.driver( "neo4j://ignore.com", auth, baseConfig()
                .withResolver( serverAddressResolver )
                .build() );
    }
}
