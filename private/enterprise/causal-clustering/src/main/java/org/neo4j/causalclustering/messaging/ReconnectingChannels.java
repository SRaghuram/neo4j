/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.stream.Streams;

public class ReconnectingChannels
{
    private final ConcurrentHashMap<AdvertisedSocketAddress,ReconnectingChannel> lazyChannelMap =
            new ConcurrentHashMap<>();

    public int size()
    {
        return lazyChannelMap.size();
    }

    public ReconnectingChannel get( AdvertisedSocketAddress to )
    {
        return lazyChannelMap.get( to );
    }

    public ReconnectingChannel putIfAbsent( AdvertisedSocketAddress to, ReconnectingChannel timestampedLazyChannel )
    {
        return lazyChannelMap.putIfAbsent( to, timestampedLazyChannel );
    }

    public Collection<ReconnectingChannel> values()
    {
        return lazyChannelMap.values();
    }

    public void remove( AdvertisedSocketAddress address )
    {
        lazyChannelMap.remove( address );
    }

    public Stream<Pair<AdvertisedSocketAddress,ProtocolStack>> installedProtocols()
    {
        return lazyChannelMap.entrySet().stream()
                .map( this::installedProtocolOpt )
                .flatMap( Streams::ofOptional );
    }

    private Optional<Pair<AdvertisedSocketAddress,ProtocolStack>> installedProtocolOpt( Map.Entry<AdvertisedSocketAddress,ReconnectingChannel> entry )
    {
        return entry.getValue().installedProtocolStack()
                .map( protocols -> Pair.of( entry.getKey(), protocols ) );
    }
}
