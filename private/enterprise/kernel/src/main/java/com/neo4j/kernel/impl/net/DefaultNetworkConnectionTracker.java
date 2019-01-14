/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.net;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.api.net.NetworkConnectionIdGenerator;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.net.TrackedNetworkConnection;

/**
 * A {@link NetworkConnectionTracker} that keeps all given connections in a {@link ConcurrentHashMap}.
 */
public class DefaultNetworkConnectionTracker implements NetworkConnectionTracker
{
    private final NetworkConnectionIdGenerator idGenerator = new NetworkConnectionIdGenerator();
    private final Map<String,TrackedNetworkConnection> connectionsById = new ConcurrentHashMap<>();

    @Override
    public String newConnectionId( String connector )
    {
        return idGenerator.newConnectionId( connector );
    }

    @Override
    public void add( TrackedNetworkConnection connection )
    {
        TrackedNetworkConnection previousConnection = connectionsById.put( connection.id(), connection );
        if ( previousConnection != null )
        {
            throw new IllegalArgumentException( "Attempt to register a connection with an existing id " + connection.id() + ". " +
                                                "Existing connection: " + previousConnection + ", new connection: " + connection );
        }
    }

    @Override
    public void remove( TrackedNetworkConnection connection )
    {
        connectionsById.remove( connection.id() );
    }

    @Override
    public TrackedNetworkConnection get( String id )
    {
        return connectionsById.get( id );
    }

    @Override
    public List<TrackedNetworkConnection> activeConnections()
    {
        return new ArrayList<>( connectionsById.values() );
    }
}
