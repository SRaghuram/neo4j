/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins;

import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingPlugin;
import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingProcessor;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.builtinprocs.routing.RoutingResult;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServerShufflingProcessorTest
{
    @Test
    public void shouldShuffleServers() throws Exception
    {
        // given
        LoadBalancingProcessor delegate = mock( LoadBalancingPlugin.class );

        List<AdvertisedSocketAddress> routers = asList(
                new AdvertisedSocketAddress( "route", 1 ),
                new AdvertisedSocketAddress( "route", 2 ) );
        List<AdvertisedSocketAddress> writers = asList(
                new AdvertisedSocketAddress( "write", 3 ),
                new AdvertisedSocketAddress( "write", 4 ),
                new AdvertisedSocketAddress( "write", 5 ) );
        List<AdvertisedSocketAddress> readers = asList(
                new AdvertisedSocketAddress( "read", 6 ),
                new AdvertisedSocketAddress( "read", 7 ),
                new AdvertisedSocketAddress( "read", 8 ),
                new AdvertisedSocketAddress( "read", 9 ) );

        long ttl = 1000;
        RoutingResult result = new RoutingResult(
                new ArrayList<>( routers ),
                new ArrayList<>( writers ),
                new ArrayList<>( readers ),
                ttl );

        when( delegate.run( any() ) ).thenReturn( result );

        ServerShufflingProcessor plugin = new ServerShufflingProcessor( delegate );

        boolean completeShuffle = false;
        for ( int i = 0; i < 1000; i++ ) // we try many times to make false negatives extremely unlikely
        {
            // when
            RoutingResult shuffledResult = plugin.run( VirtualValues.EMPTY_MAP );

            // then: should still contain the same endpoints
            assertThat( shuffledResult.routeEndpoints(), containsInAnyOrder( routers.toArray() ) );
            assertThat( shuffledResult.writeEndpoints(), containsInAnyOrder( writers.toArray() ) );
            assertThat( shuffledResult.readEndpoints(), containsInAnyOrder( readers.toArray() ) );
            assertEquals( shuffledResult.ttlMillis(), ttl );

            // but possibly in a different order
            boolean readersEqual = shuffledResult.readEndpoints().equals( readers );
            boolean writersEqual = shuffledResult.writeEndpoints().equals( writers );
            boolean routersEqual = shuffledResult.routeEndpoints().equals( routers );

            if ( !readersEqual && !writersEqual && !routersEqual )
            {
                // we don't stop until it is completely different
                completeShuffle = true;
                break;
            }
        }

        assertTrue( completeShuffle );
    }
}
