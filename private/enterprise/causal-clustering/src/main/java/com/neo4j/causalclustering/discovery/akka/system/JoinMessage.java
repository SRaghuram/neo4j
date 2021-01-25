/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.actor.Address;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class JoinMessage
{
    private JoinMessage( boolean resolveRequired, List<Address> addresses )
    {
        this.resolveRequired = resolveRequired;
        this.addresses = List.copyOf( addresses );
    }

    static JoinMessage initial( Collection<Address> addresses )
    {
        return new JoinMessage( true, new ArrayList<>( addresses ) );
    }

    private final boolean resolveRequired;
    private final List<Address> addresses;

    /**
     * Returns a new JoinMessage with the addresses rotated by moving the first address to the end of the list. We do this because Akka treats the first address
     * in the list as a special case and we want to try with each address in turn as first element.
     *
     * @return a new JoinMessage with the first address in the list moved to the back of the list (and the element that was second in the list is now first).
     */
    JoinMessage withRotatedAddresses()
    {
        var newAddresses = new LinkedList<>( addresses );
        newAddresses.addLast( newAddresses.removeFirst() );
        return new JoinMessage( false, newAddresses );
    }

    /**
     * Returns a new JoinMessage with the seed nodes added to the front of the list of addresses that we already have, and any duplicate addresses removed.
     *
     * @param seedNodes the seed nodes to attempt to join first.
     * @return a new JoinMessage containing both the seed nodes and the addresses that this message already contained.
     */
    JoinMessage withResolvedAddresses( Collection<Address> seedNodes )
    {
        var newAddresses = Stream.concat( seedNodes.stream(), addresses.stream().filter( a -> !seedNodes.contains( a ) ) )
                                 .collect( Collectors.toList() );
        return new JoinMessage( newAddresses.isEmpty(), newAddresses );
    }

    boolean hasAddress()
    {
        return !addresses.isEmpty();
    }

    List<Address> all()
    {
        return List.copyOf( addresses );
    }

    boolean isResolveRequired()
    {
        return resolveRequired;
    }

    @Override
    public String toString()
    {
        return "JoinMessage{" + "resolveRequired=" + resolveRequired + ", addresses=" + addresses + '}';
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
        JoinMessage that = (JoinMessage) o;
        return resolveRequired == that.resolveRequired && Objects.equals( addresses, that.addresses );
    }

    @Override
    public int hashCode()
    {

        return Objects.hash( resolveRequired, addresses );
    }
}
