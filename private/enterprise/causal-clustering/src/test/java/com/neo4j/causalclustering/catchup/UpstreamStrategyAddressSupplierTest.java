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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UpstreamStrategyAddressSupplierTest
{
    private MemberId defaultMember = new MemberId( UUID.randomUUID() );
    private MemberId firstMember = new MemberId( UUID.randomUUID() );
    private MemberId secondMember = new MemberId( UUID.randomUUID() );
    private AdvertisedSocketAddress defaultAddress = new AdvertisedSocketAddress( "Default", 123 );
    private AdvertisedSocketAddress firstAddress = new AdvertisedSocketAddress( "First", 456 );
    private AdvertisedSocketAddress secondAddress = new AdvertisedSocketAddress( "Second", 789 );
    private TopologyService topologyService = mock( TopologyService.class );

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws CatchupAddressResolutionException
    {
        when( topologyService.findCatchupAddress( eq( defaultMember ) ) ).thenReturn( defaultAddress );
        when( topologyService.findCatchupAddress( eq( firstMember ) ) ).thenReturn( firstAddress );
        when( topologyService.findCatchupAddress( eq( secondMember ) ) ).thenReturn( secondAddress );
    }

    @Test
    public void selectionPrioritiesAreKept() throws CatchupAddressResolutionException
    {
        // given various strategies with different priorities
        UpstreamDatabaseStrategySelector upstreamDatabaseStrategySelector =
                new UpstreamDatabaseStrategySelector( new CountedSelectionStrategy( defaultMember, 5 ),
                        Arrays.asList( new CountedSelectionStrategy( firstMember, 1 ), new CountedSelectionStrategy( secondMember, 1 ) ),
                        NullLogProvider.getInstance() );

        // and
        UpstreamStrategyAddressSupplier upstreamStrategyAddressSupplier =
                new UpstreamStrategyAddressSupplier( upstreamDatabaseStrategySelector, topologyService );

        // when
        AdvertisedSocketAddress firstResult = upstreamStrategyAddressSupplier.get();
        AdvertisedSocketAddress secondResult = upstreamStrategyAddressSupplier.get();
        AdvertisedSocketAddress thirdResult = upstreamStrategyAddressSupplier.get();

        // then
        assertEquals( firstAddress, firstResult );
        assertEquals( secondAddress, secondResult );
        assertEquals( defaultAddress, thirdResult );
    }

    @Test
    public void exceptionWhenStrategiesFail() throws CatchupAddressResolutionException
    {
        // given a guaranteed fail strategy
        UpstreamDatabaseStrategySelector upstreamDatabaseStrategySelector =
                new UpstreamDatabaseStrategySelector( new CountedSelectionStrategy( defaultMember, 0 ) );

        // and
        UpstreamStrategyAddressSupplier upstreamStrategyAddressSupplier =
                new UpstreamStrategyAddressSupplier( upstreamDatabaseStrategySelector, topologyService );

        // then
        expectedException.expect( CatchupAddressResolutionException.class );

        // when
        upstreamStrategyAddressSupplier.get();
    }

    private class CountedSelectionStrategy extends UpstreamDatabaseSelectionStrategy
    {
        MemberId upstreamDatabase;
        private int numberOfIterations;

        CountedSelectionStrategy( MemberId upstreamDatabase, int numberOfIterations )
        {
            super( CountedSelectionStrategy.class.getName() );
            this.upstreamDatabase = upstreamDatabase;
            this.numberOfIterations = numberOfIterations;
        }

        @Override
        public Optional<MemberId> upstreamDatabase()
        {
            MemberId consumed = upstreamDatabase;
            numberOfIterations--;
            if ( numberOfIterations < 0 )
            {
                upstreamDatabase = null;
            }
            return Optional.ofNullable( consumed );
        }

        @Override
        public int hashCode()
        {
            return super.hashCode() + (upstreamDatabase.hashCode() * 17) + (31 * numberOfIterations);
        }

        @Override
        public boolean equals( Object o )
        {
            if ( o == null || !(o instanceof CountedSelectionStrategy) )
            {
                return false;
            }
            CountedSelectionStrategy other = (CountedSelectionStrategy) o;
            return this.upstreamDatabase.equals( other.upstreamDatabase ) && this.numberOfIterations == other.numberOfIterations;
        }
    }
}
