/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UpstreamAddressLookupTest
{
    private final NamedDatabaseId namedDatabaseId = new TestDatabaseIdRepository().defaultDatabase();
    private final MemberId defaultMember = new MemberId( UUID.randomUUID() );
    private final MemberId firstMember = new MemberId( UUID.randomUUID() );
    private final MemberId secondMember = new MemberId( UUID.randomUUID() );
    private final SocketAddress defaultAddress = new SocketAddress( "Default", 123 );
    private final SocketAddress firstAddress = new SocketAddress( "First", 456 );
    private final SocketAddress secondAddress = new SocketAddress( "Second", 789 );
    private final TopologyService topologyService = mock( TopologyService.class );

    @BeforeEach
    void setup() throws CatchupAddressResolutionException
    {
        when( topologyService.lookupCatchupAddress( eq( defaultMember ) ) ).thenReturn( defaultAddress );
        when( topologyService.lookupCatchupAddress( eq( firstMember ) ) ).thenReturn( firstAddress );
        when( topologyService.lookupCatchupAddress( eq( secondMember ) ) ).thenReturn( secondAddress );
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
        SocketAddress firstResult = upstreamAddressLookup.lookupAddressForDatabase( namedDatabaseId );
        SocketAddress secondResult = upstreamAddressLookup.lookupAddressForDatabase( namedDatabaseId );
        SocketAddress thirdResult = upstreamAddressLookup.lookupAddressForDatabase( namedDatabaseId );

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
        assertThrows( CatchupAddressResolutionException.class, () -> upstreamAddressLookup.lookupAddressForDatabase( namedDatabaseId ) );
    }

    @Test
    void multiLookupShouldReturnAllAddressFromSingleStrategy() throws CatchupAddressResolutionException
    {
        // given
        var strategySelector = new UpstreamDatabaseStrategySelector(
                new RandomCountedSelectionStrategy( 1, secondMember, defaultMember ),
                List.of( new RandomCountedSelectionStrategy(1, firstMember, secondMember ) ), NullLogProvider.getInstance() );

        var upstreamAddressLookup = new UpstreamAddressLookup( strategySelector, topologyService );

        // when
        var firstResults = upstreamAddressLookup.lookupAddressesForDatabase( namedDatabaseId );
        var secondResults = upstreamAddressLookup.lookupAddressesForDatabase( namedDatabaseId );

        // then
        assertThat( firstResults, Matchers.containsInAnyOrder( firstAddress, secondAddress ) );
        assertThat( firstResults, Matchers.hasSize( 2 ) );
        assertThat( secondResults, Matchers.containsInAnyOrder( secondAddress, defaultAddress ) );
        assertThat( secondResults, Matchers.hasSize( 2 ) );
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
        public Optional<MemberId> upstreamMemberForDatabase( NamedDatabaseId namedDatabaseId )
        {
            if ( numberOfIterations <= 0 )
            {
                return Optional.empty();
            }
            numberOfIterations--;
            return Optional.of( upstreamDatabase );
        }
    }

    private static class RandomCountedSelectionStrategy extends UpstreamDatabaseSelectionStrategy
    {
        private final List<MemberId> members;
        private int numberOfIterations;

        RandomCountedSelectionStrategy( int numberOfIterations, MemberId... members )
        {
            super( "RandomSelectionStrategy" );
            this.members = List.of( members );
            this.numberOfIterations = numberOfIterations;
        }

        private boolean iterationsReached()
        {
            return numberOfIterations-- <= 0;
        }

        @Override
        public Optional<MemberId> upstreamMemberForDatabase( NamedDatabaseId namedDatabaseId ) throws UpstreamDatabaseSelectionException
        {
            if ( iterationsReached() )
            {
                return Optional.empty();
            }

            var choices = new ArrayList<>( members );
            Collections.shuffle( choices );
            return choices.stream().findFirst();
        }

        @Override
        public Collection<MemberId> upstreamMembersForDatabase( NamedDatabaseId namedDatabaseId ) throws UpstreamDatabaseSelectionException
        {
            if ( iterationsReached() )
            {
                return List.of();
            }

            var choices = new ArrayList<>( members );
            Collections.shuffle( choices );
            return choices;
        }
    }
}
