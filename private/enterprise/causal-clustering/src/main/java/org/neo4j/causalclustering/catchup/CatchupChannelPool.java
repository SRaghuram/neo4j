/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import java.net.ConnectException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.helpers.AdvertisedSocketAddress;

import static java.util.stream.Stream.concat;

class CatchupChannelPool<CHANNEL extends CatchupChannelPool.Channel>
{
    private final Map<AdvertisedSocketAddress,LinkedList<CHANNEL>> idleChannels = new HashMap<>();
    private final Set<CHANNEL> activeChannels = new HashSet<>();
    private final Function<AdvertisedSocketAddress,CHANNEL> factory;

    CatchupChannelPool( Function<AdvertisedSocketAddress,CHANNEL> factory )
    {
        this.factory = factory;
    }

    CHANNEL acquire( AdvertisedSocketAddress catchUpAddress ) throws Exception
    {
        CHANNEL channel = getIdleChannel( catchUpAddress );

        if ( channel == null )
        {
            channel = factory.apply( catchUpAddress );
            try
            {
                channel.connect();
                assertActive( channel, catchUpAddress );
            }
            catch ( Exception e )
            {
                channel.close();
                throw e;
            }
        }

        addActiveChannel( channel );

        return channel;
    }

    private void assertActive( CHANNEL channel, AdvertisedSocketAddress address ) throws ConnectException
    {
        if ( !channel.isActive() )
        {
            throw new ConnectException( "Unable to connect to " + address );
        }
    }

    private synchronized CHANNEL getIdleChannel( AdvertisedSocketAddress catchUpAddress )
    {
        CHANNEL channel = null;
        LinkedList<CHANNEL> channels = idleChannels.get( catchUpAddress );
        if ( channels != null )
        {
            while ( (channel = channels.poll()) != null )
            {
                if ( channel.isActive() )
                {
                    break;
                }
            }
            if ( channels.isEmpty() )
            {
                idleChannels.remove( catchUpAddress );
            }
        }
        return channel;
    }

    private synchronized void addActiveChannel( CHANNEL channel )
    {
        activeChannels.add( channel );
    }

    private synchronized void removeActiveChannel( CHANNEL channel )
    {
        activeChannels.remove( channel );
    }

    void dispose( CHANNEL channel )
    {
        removeActiveChannel( channel );
        channel.close();
    }

    synchronized void release( CHANNEL channel )
    {
        removeActiveChannel( channel );
        idleChannels.computeIfAbsent( channel.destination(), address -> new LinkedList<>() ).add( channel );
    }

    void close()
    {
        collectDisposed().forEach( Channel::close );
    }

    private synchronized Set<CHANNEL> collectDisposed()
    {
        Set<CHANNEL> disposed;
        disposed = concat(
                idleChannels.values().stream().flatMap( Collection::stream ),
                activeChannels.stream() )
                .collect( Collectors.toSet() );

        idleChannels.clear();
        activeChannels.clear();
        return disposed;
    }

    interface Channel
    {
        AdvertisedSocketAddress destination();

        void connect() throws Exception;

        boolean isActive();

        void close();
    }
}
