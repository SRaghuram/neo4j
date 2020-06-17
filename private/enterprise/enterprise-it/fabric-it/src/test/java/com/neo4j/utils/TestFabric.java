/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.utils;

import com.neo4j.TestServer;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.harness.Neo4j;

public class TestFabric implements AutoCloseable
{
    private final List<Neo4j> shards;
    private final TestServer testServer;

    private final Map<Integer,Driver> shardDrivers = new HashMap<>();
    private Driver routingClientDriver;
    private Driver directClientDriver;

    TestFabric( TestServer testServer, List<Neo4j> shards )
    {
        this.testServer = testServer;
        this.shards = shards;
    }

    public Neo4j getShard( int shardId )
    {
        return shards.get( shardId );
    }

    public Driver driverForShard( int shardId )
    {
        return shardDrivers.computeIfAbsent( shardId, id ->
        {
            var shard = shards.get( id );
            return createDriver( shard.boltURI() );
        } );
    }

    public URI getBoltDirectUri()
    {
        return testServer.getBoltDirectUri();
    }

    public URI getBoltRoutingUri()
    {
        return testServer.getBoltRoutingUri();
    }

    public Driver routingClientDriver()
    {
        if ( routingClientDriver == null )
        {
            routingClientDriver = createDriver( testServer.getBoltRoutingUri() );
        }

        return routingClientDriver;
    }

    public TestServer getTestServer()
    {
        return testServer;
    }

    public Driver directClientDriver()
    {
        if ( directClientDriver == null )
        {
            directClientDriver = createDriver( testServer.getBoltDirectUri() );
        }

        return directClientDriver;
    }

    private Driver createDriver( URI uri )
    {
        return GraphDatabase.driver(
                uri,
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                                       .withoutEncryption()
                                       .build() );
    }

    @Override
    public void close()
    {
        List<Runnable> closables = new ArrayList<>();

        closables.add( testServer::stop );
        shards.forEach( shard -> closables.add( shard::close ) );
        shardDrivers.values().forEach( driver -> closables.add( driver::close ) );
        if ( routingClientDriver != null )
        {
            closables.add( routingClientDriver::close );
        }

        if ( directClientDriver != null )
        {
            closables.add( directClientDriver::close );
        }

        closables.parallelStream().forEach( Runnable::run );
    }
}
