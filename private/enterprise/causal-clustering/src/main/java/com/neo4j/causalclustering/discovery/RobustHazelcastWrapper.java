/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.hazelcast.core.HazelcastInstance;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A class which attempts to capture behaviours necessary to make interacting
 * with hazelcast robust, e.g. reconnect on failures. This class is not aimed
 * at high-performance and thus uses synchronization heavily.
 */
class RobustHazelcastWrapper
{
    private final HazelcastConnector connector;
    private HazelcastInstance hzInstance;
    private boolean shutdown;

    RobustHazelcastWrapper( HazelcastConnector connector )
    {
        this.connector = connector;
    }

    synchronized void shutdown()
    {
        if ( hzInstance != null )
        {
            hzInstance.shutdown();
            hzInstance = null;
            shutdown = true;
        }
    }

    private synchronized HazelcastInstance tryEnsureConnection() throws HazelcastInstanceNotActiveException
    {
        if ( shutdown )
        {
            throw new HazelcastInstanceNotActiveException( "Shutdown" );
        }

        if ( hzInstance == null )
        {
            hzInstance = connector.connectToHazelcast();
        }
        return hzInstance;
    }

    private synchronized void invalidateConnection()
    {
        hzInstance = null;
    }

    synchronized <T> T apply( Function<HazelcastInstance,T> function ) throws HazelcastInstanceNotActiveException
    {
        HazelcastInstance hzInstance = tryEnsureConnection();

        try
        {
            return function.apply( hzInstance );
        }
        catch ( com.hazelcast.core.HazelcastInstanceNotActiveException e )
        {
            invalidateConnection();
            throw new HazelcastInstanceNotActiveException( e );
        }
    }

    synchronized void perform( Consumer<HazelcastInstance> operation ) throws HazelcastInstanceNotActiveException
    {
        HazelcastInstance hzInstance = tryEnsureConnection();

        try
        {
            operation.accept( hzInstance );
        }
        catch ( com.hazelcast.core.HazelcastInstanceNotActiveException e )
        {
            invalidateConnection();
            throw new HazelcastInstanceNotActiveException( e );
        }
    }
}
