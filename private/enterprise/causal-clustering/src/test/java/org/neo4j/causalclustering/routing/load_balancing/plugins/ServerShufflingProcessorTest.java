/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.routing.load_balancing.plugins;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.causalclustering.routing.Endpoint;
import org.neo4j.causalclustering.routing.load_balancing.LoadBalancingPlugin;
import org.neo4j.causalclustering.routing.load_balancing.LoadBalancingProcessor;
import org.neo4j.causalclustering.routing.load_balancing.LoadBalancingResult;
import org.neo4j.helpers.AdvertisedSocketAddress;

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

        List<Endpoint> routers = asList(
                Endpoint.route( new AdvertisedSocketAddress( "route", 1 ) ),
                Endpoint.route( new AdvertisedSocketAddress( "route", 2 ) ) );
        List<Endpoint> writers = asList(
                Endpoint.write( new AdvertisedSocketAddress( "write", 3 ) ),
                Endpoint.write( new AdvertisedSocketAddress( "write", 4 ) ),
                Endpoint.write( new AdvertisedSocketAddress( "write", 5 ) ) );
        List<Endpoint> readers = asList(
                Endpoint.read( new AdvertisedSocketAddress( "read", 6 ) ),
                Endpoint.read( new AdvertisedSocketAddress( "read", 7 ) ),
                Endpoint.read( new AdvertisedSocketAddress( "read", 8 ) ),
                Endpoint.read( new AdvertisedSocketAddress( "read", 9 ) ) );

        long ttl = 1000;
        LoadBalancingProcessor.Result result = new LoadBalancingResult(
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
            LoadBalancingProcessor.Result shuffledResult = plugin.run( Collections.emptyMap() );

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
