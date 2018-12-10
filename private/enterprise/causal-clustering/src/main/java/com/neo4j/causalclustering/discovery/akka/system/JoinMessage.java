/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.actor.Address;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

class JoinMessage
{
    private JoinMessage( boolean isReJoin, List<Address> addresses )
    {
        this.isReJoin = isReJoin;
        this.addresses = Collections.unmodifiableList( addresses );
    }

    static JoinMessage initial( boolean isReJoin, Collection<Address> addresses )
    {
        return new JoinMessage( isReJoin, new ArrayList<>( addresses ) );
    }

    private final boolean isReJoin;
    private final List<Address> addresses;

    JoinMessage tailMsg()
    {
        return new JoinMessage( isReJoin, addresses.subList( 1, addresses.size() ) );
    }

    boolean hasAddress()
    {
        return !addresses.isEmpty();
    }

    Address head()
    {
        return addresses.get( 0 );
    }

    boolean isReJoin()
    {
        return isReJoin;
    }

    @Override
    public String toString()
    {
        return "JoinMessage{" + "isReJoin=" + isReJoin + ", addresses=" + addresses + '}';
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
        return isReJoin == that.isReJoin && Objects.equals( addresses, that.addresses );
    }

    @Override
    public int hashCode()
    {

        return Objects.hash( isReJoin, addresses );
    }
}
