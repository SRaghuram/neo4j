/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.multi_cluster.procedure;

import com.neo4j.causalclustering.routing.multi_cluster.MultiClusterRoutingResult;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.values.AnyValue;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class MultiClusterRoutingResultFormatTest
{

    @Test
    public void shouldSerializeToAndFromRecordFormat()
    {
        List<AdvertisedSocketAddress> fooRouters = asList(
                new AdvertisedSocketAddress( "host1", 1 ),
                new AdvertisedSocketAddress( "host2", 1 ),
                new AdvertisedSocketAddress( "host3", 1 )
        );

        List<AdvertisedSocketAddress> barRouters = asList(
                new AdvertisedSocketAddress( "host4", 1 ),
                new AdvertisedSocketAddress( "host5", 1 ),
                new AdvertisedSocketAddress( "host6", 1 )
        );

        Map<DatabaseId,List<AdvertisedSocketAddress>> routers = new HashMap<>();
        routers.put( new DatabaseId( "foo" ), fooRouters );
        routers.put( new DatabaseId( "bar" ), barRouters );

        long ttlSeconds = 5;
        MultiClusterRoutingResult original = new MultiClusterRoutingResult( routers, ttlSeconds * 1000 );

        AnyValue[] record = MultiClusterRoutingResultFormat.build( original );

        MultiClusterRoutingResult parsed = MultiClusterRoutingResultFormat.parse( record );

        assertEquals( original, parsed );
    }
}
