/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
}
