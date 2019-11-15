/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import com.neo4j.causalclustering.common.Cluster;

import java.net.URI;
import java.util.stream.Collectors;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.net.ServerAddress;

public class BoltDriverHelper
{
    private static final Config.ConfigBuilder TEST_DRIVER_CONFIG = Config.builder().withoutEncryption().withLogging( Logging.none() );

    public static Driver graphDatabaseDriver( URI uri )
    {
        return GraphDatabase.driver( uri, TEST_DRIVER_CONFIG.build() );
    }

    public static Driver graphDatabaseDriver( String uri )
    {
        return GraphDatabase.driver( uri, TEST_DRIVER_CONFIG.build() );
    }

    public static Driver graphDatabaseDriver( URI uri, AuthToken auth )
    {
        return GraphDatabase.driver( uri, auth, TEST_DRIVER_CONFIG.build() );
    }

    public static Driver graphDatabaseDriver( String uri, AuthToken auth )
    {
        return GraphDatabase.driver( uri, auth, TEST_DRIVER_CONFIG.build() );
    }

    public static Driver graphDatabaseDriver( Cluster cluster, AuthToken auth )
    {
        return GraphDatabase.driver( "neo4j://ignore.com", auth, TEST_DRIVER_CONFIG
                .withResolver( address -> cluster
                        .coreMembers()
                        .stream()
                        .map( c -> URI.create( c.routingURI() ) )
                        .map( uri -> ServerAddress.of( uri.getHost(), uri.getPort() ) )
                        .collect( Collectors.toSet() ) )
                .build() );
    }
}
