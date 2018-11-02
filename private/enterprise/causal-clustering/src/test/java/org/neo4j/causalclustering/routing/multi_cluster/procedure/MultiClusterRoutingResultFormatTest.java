/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.routing.multi_cluster.procedure;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.causalclustering.routing.Endpoint;
import org.neo4j.causalclustering.routing.multi_cluster.MultiClusterRoutingResult;
import org.neo4j.helpers.AdvertisedSocketAddress;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class MultiClusterRoutingResultFormatTest
{

    @Test
    public void shouldSerializeToAndFromRecordFormat()
    {
        List<Endpoint> fooRouters = asList(
                Endpoint.route( new AdvertisedSocketAddress( "host1", 1 ) ),
                Endpoint.route( new AdvertisedSocketAddress( "host2", 1 ) ),
                Endpoint.route( new AdvertisedSocketAddress( "host3", 1 ) )
        );

        List<Endpoint> barRouters = asList(
                Endpoint.route( new AdvertisedSocketAddress( "host4", 1 ) ),
                Endpoint.route( new AdvertisedSocketAddress( "host5", 1 ) ),
                Endpoint.route( new AdvertisedSocketAddress( "host6", 1 ) )
        );

        Map<String,List<Endpoint>> routers = new HashMap<>();
        routers.put( "foo", fooRouters );
        routers.put( "bar", barRouters );

        long ttlSeconds = 5;
        MultiClusterRoutingResult original = new MultiClusterRoutingResult( routers, ttlSeconds * 1000 );

        Object[] record = MultiClusterRoutingResultFormat.build( original );

        MultiClusterRoutingResult parsed = MultiClusterRoutingResultFormat.parse( record );

        assertEquals( original, parsed );
    }
}
