/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream;

import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.upstream.strategies.ConnectToRandomCoreServerStrategy;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.NullLogProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.Iterables.iterable;

public class UpstreamDatabaseStrategySelectorTest
{
    private static final DatabaseId DATABASE_ID = new DatabaseId( "clients" );

    @Test
    void shouldReturnTheMemberIdFromFirstSuccessfulStrategy() throws Exception
    {
        // given
        UpstreamDatabaseSelectionStrategy badOne = mock( UpstreamDatabaseSelectionStrategy.class );
        when( badOne.upstreamMemberForDatabase( DATABASE_ID ) ).thenReturn( Optional.empty() );

        UpstreamDatabaseSelectionStrategy anotherBadOne = mock( UpstreamDatabaseSelectionStrategy.class );
        when( anotherBadOne.upstreamMemberForDatabase( DATABASE_ID ) ).thenReturn( Optional.empty() );

        UpstreamDatabaseSelectionStrategy goodOne = mock( UpstreamDatabaseSelectionStrategy.class );
        MemberId theMemberId = new MemberId( UUID.randomUUID() );
        when( goodOne.upstreamMemberForDatabase( DATABASE_ID ) ).thenReturn( Optional.of( theMemberId ) );

        UpstreamDatabaseStrategySelector selector =
                new UpstreamDatabaseStrategySelector( badOne, iterable( goodOne, anotherBadOne ), NullLogProvider.getInstance() );

        // when
        MemberId result = selector.bestUpstreamMemberForDatabase( DATABASE_ID );

        // then
        assertEquals( theMemberId, result );
    }

    @Test
    void shouldDefaultToRandomCoreServerIfNoOtherStrategySpecified() throws Exception
    {
        // given
        TopologyService topologyService = mock( TopologyService.class );
        MemberId memberId = new MemberId( UUID.randomUUID() );
        when( topologyService.coreTopologyForDatabase( DATABASE_ID ) ).thenReturn(
                new DatabaseCoreTopology( DATABASE_ID, new RaftId( UUID.randomUUID() ), Map.of( memberId, mock( CoreServerInfo.class ) ) ) );

        ConnectToRandomCoreServerStrategy defaultStrategy = new ConnectToRandomCoreServerStrategy();
        defaultStrategy.inject( topologyService, Config.defaults(), NullLogProvider.getInstance(), null );

        UpstreamDatabaseStrategySelector selector = new UpstreamDatabaseStrategySelector( defaultStrategy );

        // when
        MemberId instance = selector.bestUpstreamMemberForDatabase( DATABASE_ID );

        // then
        assertEquals( memberId, instance );
    }

    @Test
    void shouldUseSpecifiedStrategyInPreferenceToDefault() throws Exception
    {
        // given
        TopologyService topologyService = mock( TopologyService.class );
        MemberId memberId = new MemberId( UUID.randomUUID() );
        when( topologyService.coreTopologyForDatabase( DATABASE_ID ) ).thenReturn(
                new DatabaseCoreTopology( DATABASE_ID, new RaftId( UUID.randomUUID() ), Map.of( memberId, mock( CoreServerInfo.class ) ) ) );

        ConnectToRandomCoreServerStrategy shouldNotUse = mock( ConnectToRandomCoreServerStrategy.class );

        UpstreamDatabaseSelectionStrategy mockStrategy = mock( UpstreamDatabaseSelectionStrategy.class );
        when( mockStrategy.upstreamMemberForDatabase( DATABASE_ID ) ).thenReturn( Optional.of( new MemberId( UUID.randomUUID() ) ) );

        UpstreamDatabaseStrategySelector selector =
                new UpstreamDatabaseStrategySelector( shouldNotUse, iterable( mockStrategy ), NullLogProvider.getInstance() );

        // when
        selector.bestUpstreamMemberForDatabase( DATABASE_ID );

        // then
        verifyZeroInteractions( shouldNotUse );
    }

    @ServiceProvider
    public static class DummyUpstreamDatabaseSelectionStrategy extends UpstreamDatabaseSelectionStrategy
    {
        private MemberId memberId;

        public DummyUpstreamDatabaseSelectionStrategy()
        {
            super( "dummy" );
        }

        @Override
        public Optional<MemberId> upstreamMemberForDatabase( DatabaseId databaseId )
        {
            return Optional.ofNullable( memberId );
        }

        public void setMemberId( MemberId memberId )
        {
            this.memberId = memberId;
        }
    }

    @ServiceProvider
    public static class AnotherDummyUpstreamDatabaseSelectionStrategy extends UpstreamDatabaseSelectionStrategy
    {
        public AnotherDummyUpstreamDatabaseSelectionStrategy()
        {
            super( "another-dummy" );
        }

        @Override
        public Optional<MemberId> upstreamMemberForDatabase( DatabaseId databaseId )
        {
            return Optional.of( new MemberId( UUID.randomUUID() ) );
        }
    }

    @ServiceProvider
    public static class YetAnotherDummyUpstreamDatabaseSelectionStrategy extends UpstreamDatabaseSelectionStrategy
    {
        public YetAnotherDummyUpstreamDatabaseSelectionStrategy()
        {
            super( "yet-another-dummy" );
        }

        @Override
        public Optional<MemberId> upstreamMemberForDatabase( DatabaseId databaseId )
        {
            return Optional.of( new MemberId( UUID.randomUUID() ) );
        }
    }
}
