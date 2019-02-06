/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.connection;

import java.io.Closeable;
import java.net.URI;
import java.util.function.Supplier;

import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Logging;

public class DriverSupplier implements Supplier<Driver>, Closeable
{
    private final Logging logging;
    private final URI uri;
    private final AuthToken authToken;

    private Driver driver;

    public DriverSupplier( Logging logging, URI uri, AuthToken authToken )
    {
        this.logging = logging;
        this.uri = uri;
        this.authToken = authToken;
    }

    @Override
    public Driver get()
    {
        if ( driver == null )
        {
            this.driver = GraphDatabase.driver( uri, authToken, Config.build().withLogging( logging )
                                                                      .withEncryptionLevel( Config.EncryptionLevel.NONE )
                                                                      .toConfig() );
        }
        return driver;
    }

    @Override
    public void close()
    {
        if ( driver != null )
        {
            driver.close();
        }
    }
}
