/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.IdFactory;
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

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.identity.ServerId;
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
    private final ServerId defaultServer = IdFactory.randomServerId();
    private final ServerId firstServer = IdFactory.randomServerId();
    private final ServerId secondServer = IdFactory.randomServerId();
    private final SocketAddress defaultAddress = new SocketAddress( "Default", 123 );
    private final SocketAddress firstAddress = new SocketAddress( "First", 456 );
    private final SocketAddress secondAddress = new SocketAddress( "Second", 789 );
    private final TopologyService topologyService = mock( TopologyService.class );

    @BeforeEach
    void setup() throws CatchupAddressResolutionException
    {
        when( topologyService.lookupCatchupAddress( eq( defaultServer ) ) ).thenReturn( defaultAddress );
        when( topologyService.lookupCatchupAddress( eq( firstServer ) ) ).thenReturn( firstAddress );
        when( topologyService.lookupCatchupAddress( eq( secondServer ) ) ).thenReturn( secondAddress );
    }

    @Test
    void selectionPrioritiesAreKept() throws CatchupAddressResolutionException
    {
        // given various strategies with different priorities
        UpstreamDatabaseStrategySelector upstreamDatabaseStrategySelector =
                new UpstreamDatabaseStrategySelector( new CountedSelectionStrategy( defaultServer, 5 ),
                        Arrays.asList( new CountedSelectionStrategy( firstServer, 1 ), new CountedSelectionStrategy( secondServer, 1 ) ),
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
                new UpstreamDatabaseStrategySelector( new CountedSelectionStrategy( defaultServer, 0 ) );

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
                new RandomCountedSelectionStrategy( 1, secondServer, defaultServer ),
                List.of( new RandomCountedSelectionStrategy( 1, firstServer, secondServer ) ), NullLogProvider.getInstance() );

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
        private final ServerId upstreamDatabase;
        private int numberOfIterations;

        CountedSelectionStrategy( ServerId upstreamDatabase, int numberOfIterations )
        {
            super( CountedSelectionStrategy.class.getName() );
            this.upstreamDatabase = upstreamDatabase;
            this.numberOfIterations = numberOfIterations;
        }

        @Override
        public Optional<ServerId> upstreamServerForDatabase( NamedDatabaseId namedDatabaseId )
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
        private final List<ServerId> servers;
        private int numberOfIterations;

        RandomCountedSelectionStrategy( int numberOfIterations, ServerId... servers )
        {
            super( "RandomSelectionStrategy" );
            this.servers = List.of( servers );
            this.numberOfIterations = numberOfIterations;
        }

        private boolean iterationsReached()
        {
            return numberOfIterations-- <= 0;
        }

        @Override
        public Optional<ServerId> upstreamServerForDatabase( NamedDatabaseId namedDatabaseId ) throws UpstreamDatabaseSelectionException
        {
            if ( iterationsReached() )
            {
                return Optional.empty();
            }

            var choices = new ArrayList<>( servers );
            Collections.shuffle( choices );
            return choices.stream().findFirst();
        }

        @Override
        public Collection<ServerId> upstreamServersForDatabase( NamedDatabaseId namedDatabaseId ) throws UpstreamDatabaseSelectionException
        {
            if ( iterationsReached() )
            {
                return List.of();
            }

            var choices = new ArrayList<>( servers );
            Collections.shuffle( choices );
            return choices;
        }
    }
}
