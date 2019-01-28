/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.procedure;

import com.neo4j.causalclustering.routing.Endpoint;
import com.neo4j.causalclustering.routing.Role;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.ListValue;

public final class RoutingResultFormatHelper
{

    public static List<Endpoint> parseEndpoints( ListValue addresses, Role role )
    {
        List<Endpoint> endpoints = new ArrayList<>( addresses.size() );
        for ( AnyValue address : addresses )
        {
            endpoints.add( new Endpoint( parseAddress( (TextValue) address ), role ) );
        }
        return endpoints;
    }

    private static AdvertisedSocketAddress parseAddress( TextValue address )
    {
        ListValue split = address.split( ":" );
        return new AdvertisedSocketAddress( ((TextValue) split.value( 0 )).stringValue(),
                Integer.valueOf( ((TextValue) split.value( 1 ) ).stringValue()) );
    }
}
