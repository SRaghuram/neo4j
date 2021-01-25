/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import java.net.URI;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;

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
}
