/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.protocol.Protocol;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.helpers.collection.Pair;

public abstract class ProtocolRepository<U extends Comparable<U>,T extends Protocol<U>>
{
    private final Map<Pair<String, U>,T> protocolMap;
    private final BiFunction<String,Set<U>,ProtocolSelection<U,T>> protocolSelectionFactory;
    private Function<String,Comparator<T>> comparator;

    public ProtocolRepository( T[] protocols, Function<String,Comparator<T>> comparators,
            BiFunction<String,Set<U>,ProtocolSelection<U,T>> protocolSelectionFactory )
    {
        this.protocolSelectionFactory = protocolSelectionFactory;
        Map<Pair<String, U>,T> map = new HashMap<>();
        for ( T protocol : protocols )
        {
            Protocol<U> previous = map.put( Pair.of( protocol.category(), protocol.implementation() ), protocol );
            if ( previous != null )
            {
                throw new IllegalArgumentException(
                        String.format( "Multiple protocols with same identifier and version supplied: %s and %s", protocol, previous ) );
            }
        }
        protocolMap = Collections.unmodifiableMap( map );
        this.comparator = comparators;
    }

    Optional<T> select( String protocolName, U version )
    {
        return Optional.ofNullable( protocolMap.get( Pair.of( protocolName, version ) ) );
    }

    Optional<T> select( String protocolName, Set<U> versions )
    {
        return versions
                .stream()
                .map( version -> select( protocolName, version ) )
                .flatMap( opt -> opt.stream() )
                .max( comparator.apply( protocolName ) );
    }

    public ProtocolSelection<U,T> getAll( Protocol.Category<T> category, Collection<U> versions )
    {
        Set<U> selectedVersions = protocolMap
                .entrySet()
                .stream()
                .map( Map.Entry::getKey )
                .filter( pair -> pair.first().equals( category.canonicalName() ) )
                .map( Pair::other )
                .filter( version -> versions.isEmpty() || versions.contains( version ) )
                .collect( Collectors.toSet() );

        if ( selectedVersions.isEmpty() )
        {
            throw new IllegalArgumentException( String.format(
                    "Attempted to select protocols for %s versions %s but no match in known protocols %s", category, versions, protocolMap
            ) );
        }
        else
        {
            return protocolSelectionFactory.apply( category.canonicalName(), selectedVersions );
        }
    }

    static <U extends Comparable<U>, T extends Protocol<U>> Comparator<T> versionNumberComparator()
    {
        return Comparator.comparing( Protocol::implementation );
    }
}
