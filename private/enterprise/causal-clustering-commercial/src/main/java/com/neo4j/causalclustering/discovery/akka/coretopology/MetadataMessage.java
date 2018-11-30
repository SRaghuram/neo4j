/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.cluster.UniqueAddress;
import akka.cluster.ddata.LWWMap;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import org.neo4j.stream.Streams;
import org.neo4j.util.VisibleForTesting;

import static java.util.Collections.unmodifiableMap;

public class MetadataMessage
{
    public static final MetadataMessage EMPTY = new MetadataMessage( Collections.emptyMap() );

    private final Map<UniqueAddress,CoreServerInfoForMemberId> metadata;

    public MetadataMessage( LWWMap<UniqueAddress,CoreServerInfoForMemberId> metadata )
    {
        this( metadata.getEntries() );
    }

    /**
     * Warning: doesn't ensure inner map is immutable
     */
    @VisibleForTesting
    public MetadataMessage( Map<UniqueAddress,CoreServerInfoForMemberId> metadata )
    {
        this.metadata = unmodifiableMap( metadata );
    }

    public Optional<CoreServerInfoForMemberId> getOpt( UniqueAddress address )
    {
        return Optional.ofNullable( metadata.get( address ) );
    }

    public Stream<CoreServerInfoForMemberId> getStream( UniqueAddress address )
    {
        return Streams.ofNullable( metadata.get( address ) );
    }

    @Override
    public String toString()
    {
        return "MetadataMessage{" + "metadata=" + metadata + '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        MetadataMessage that = (MetadataMessage) o;
        return Objects.equals( metadata, that.metadata );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( metadata );
    }
}
