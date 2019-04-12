/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.NullLogProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class UpstreamAddressLookupTest
{
    private final DatabaseId databaseId = new DatabaseId( DEFAULT_DATABASE_NAME );
    private final MemberId defaultMember = new MemberId( UUID.randomUUID() );
    private final MemberId firstMember = new MemberId( UUID.randomUUID() );
    private final MemberId secondMember = new MemberId( UUID.randomUUID() );
    private final AdvertisedSocketAddress defaultAddress = new AdvertisedSocketAddress( "Default", 123 );
    private final AdvertisedSocketAddress firstAddress = new AdvertisedSocketAddress( "First", 456 );
    private final AdvertisedSocketAddress secondAddress = new AdvertisedSocketAddress( "Second", 789 );
    private final TopologyService topologyService = mock( TopologyService.class );

    @BeforeEach
    void setup() throws CatchupAddressResolutionException
    {
        when( topologyService.findCatchupAddress( eq( defaultMember ) ) ).thenReturn( defaultAddress );
        when( topologyService.findCatchupAddress( eq( firstMember ) ) ).thenReturn( firstAddress );
        when( topologyService.findCatchupAddress( eq( secondMember ) ) ).thenReturn( secondAddress );
    }

    @Test
    void selectionPrioritiesAreKept() throws CatchupAddressResolutionException
    {
        // given various strategies with different priorities
        UpstreamDatabaseStrategySelector upstreamDatabaseStrategySelector =
                new UpstreamDatabaseStrategySelector( new CountedSelectionStrategy( defaultMember, 5 ),
                        Arrays.asList( new CountedSelectionStrategy( firstMember, 1 ), new CountedSelectionStrategy( secondMember, 1 ) ),
                        NullLogProvider.getInstance() );

        // and
        UpstreamAddressLookup upstreamAddressLookup =
                new UpstreamAddressLookup( upstreamDatabaseStrategySelector, topologyService );

        // when
        AdvertisedSocketAddress firstResult = upstreamAddressLookup.lookupAddressForDatabase( databaseId );
        AdvertisedSocketAddress secondResult = upstreamAddressLookup.lookupAddressForDatabase( databaseId );
        AdvertisedSocketAddress thirdResult = upstreamAddressLookup.lookupAddressForDatabase( databaseId );

        // then
        assertEquals( firstAddress, firstResult );
        assertEquals( secondAddress, secondResult );
        assertEquals( defaultAddress, thirdResult );
    }

    @Test
    void exceptionWhenStrategiesFail()
    {
        // given a guaranteed fail strategy
        UpstreamDatabaseStrategySelector upstreamDatabaseStrategySelector =
                new UpstreamDatabaseStrategySelector( new CountedSelectionStrategy( defaultMember, 0 ) );

        // and
        UpstreamAddressLookup upstreamAddressLookup =
                new UpstreamAddressLookup( upstreamDatabaseStrategySelector, topologyService );

        // when & then
        assertThrows( CatchupAddressResolutionException.class, () -> upstreamAddressLookup.lookupAddressForDatabase( databaseId ) );
    }

    private static class CountedSelectionStrategy extends UpstreamDatabaseSelectionStrategy
    {
        private final MemberId upstreamDatabase;
        private int numberOfIterations;

        CountedSelectionStrategy( MemberId upstreamDatabase, int numberOfIterations )
        {
            super( CountedSelectionStrategy.class.getName() );
            this.upstreamDatabase = upstreamDatabase;
            this.numberOfIterations = numberOfIterations;
        }

        @Override
        public Optional<MemberId> upstreamMemberForDatabase( DatabaseId databaseId )
        {
            if ( numberOfIterations <= 0 )
            {
                return Optional.empty();
            }
            numberOfIterations--;
            return Optional.of( upstreamDatabase );
        }
    }
}
