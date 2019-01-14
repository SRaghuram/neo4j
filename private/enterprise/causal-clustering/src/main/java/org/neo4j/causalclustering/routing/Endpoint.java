/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.routing;

import java.util.Objects;

import org.neo4j.helpers.AdvertisedSocketAddress;

/**
 * This class binds a certain role with an address and
 * thus defines a reachable endpoint with defined capabilities.
 */
public class Endpoint
{
    private final AdvertisedSocketAddress address;
    private final Role role;

    public Endpoint( AdvertisedSocketAddress address, Role role )
    {
        this.address = address;
        this.role = role;
    }

    public Endpoint( AdvertisedSocketAddress address, Role role, String dbName )
    {
        this.address = address;
        this.role = role;
    }

    public AdvertisedSocketAddress address()
    {
        return address;
    }

    public static Endpoint write( AdvertisedSocketAddress address )
    {
        return new Endpoint( address, Role.WRITE );
    }

    public static Endpoint read( AdvertisedSocketAddress address )
    {
        return new Endpoint( address, Role.READ );
    }

    public static Endpoint route( AdvertisedSocketAddress address )
    {
        return new Endpoint( address, Role.ROUTE );
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
        Endpoint endPoint = (Endpoint) o;
        return Objects.equals( address, endPoint.address ) && role == endPoint.role;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( address, role );
    }

    @Override
    public String toString()
    {
        return "EndPoint{" + "address=" + address + ", role=" + role + '}';
    }
}
