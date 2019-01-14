/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.routing.procedure;

import java.util.List;
import java.util.stream.Stream;

import org.neo4j.causalclustering.routing.Endpoint;
import org.neo4j.causalclustering.routing.Role;
import org.neo4j.helpers.AdvertisedSocketAddress;

import static java.util.stream.Collectors.toList;

public final class RoutingResultFormatHelper
{

    public static List<Endpoint> parseEndpoints( Object[] addresses, Role role )
    {
        return Stream.of( addresses )
                .map( rawAddress -> parseAddress( (String) rawAddress ) )
                .map( address -> new Endpoint( address, role ) )
                .collect( toList() );
    }

    private static AdvertisedSocketAddress parseAddress( String address )
    {
        String[] split = address.split( ":" );
        return new AdvertisedSocketAddress( split[0], Integer.valueOf( split[1] ) );
    }
}
