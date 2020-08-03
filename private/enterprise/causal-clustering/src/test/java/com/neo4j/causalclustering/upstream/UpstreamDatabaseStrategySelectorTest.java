/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream;

import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.upstream.strategies.ConnectToRandomCoreServerStrategy;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.Iterables.iterable;

public class UpstreamDatabaseStrategySelectorTest
{
    private static final NamedDatabaseId NAMED_DATABASE_ID = TestDatabaseIdRepository.randomNamedDatabaseId();
    private static final DatabaseId DATABASE_ID = TestDatabaseIdRepository.randomNamedDatabaseId().databaseId();

    @Test
    void shouldReturnTheMemberIdFromFirstSuccessfulStrategy() throws Exception
    {
        // given
        var badOne = new DummyUpstreamDatabaseSelectionStrategy();
        var anotherBadOne = new DummyUpstreamDatabaseSelectionStrategy();
        var goodOne = new DummyUpstreamDatabaseSelectionStrategy();
        MemberId theMemberId = IdFactory.randomMemberId();
        goodOne.setMemberId( theMemberId );

        UpstreamDatabaseStrategySelector selector =
                new UpstreamDatabaseStrategySelector( badOne, iterable( goodOne, anotherBadOne ), NullLogProvider.getInstance() );

        // when
        MemberId result = selector.bestUpstreamMemberForDatabase( NAMED_DATABASE_ID );
        Collection<MemberId> results = selector.bestUpstreamMembersForDatabase( NAMED_DATABASE_ID );

        // then
        assertEquals( List.of( theMemberId ), results );
        assertEquals( theMemberId, result );
    }

    @Test
    void shouldDefaultToRandomCoreServerIfNoOtherStrategySpecified() throws Exception
    {
        // given
        TopologyService topologyService = mock( TopologyService.class );
        MemberId memberId = IdFactory.randomMemberId();
        when( topologyService.coreTopologyForDatabase( NAMED_DATABASE_ID ) )
                .thenReturn( new DatabaseCoreTopology( DATABASE_ID, RaftId.from( DATABASE_ID ), Map.of( memberId, mock( CoreServerInfo.class ) ) ) );

        ConnectToRandomCoreServerStrategy defaultStrategy = new ConnectToRandomCoreServerStrategy();
        defaultStrategy.inject( topologyService, Config.defaults(), NullLogProvider.getInstance(), null );

        UpstreamDatabaseStrategySelector selector = new UpstreamDatabaseStrategySelector( defaultStrategy );

        // when
        MemberId instance = selector.bestUpstreamMemberForDatabase( NAMED_DATABASE_ID );
        Collection<MemberId> instances = selector.bestUpstreamMembersForDatabase( NAMED_DATABASE_ID );

        // then
        assertEquals( memberId, instance );
        assertEquals( List.of( memberId ), instances );
    }

    @Test
    void shouldUseSpecifiedStrategyInPreferenceToDefault() throws Exception
    {
        // given
        TopologyService topologyService = mock( TopologyService.class );
        MemberId memberId = IdFactory.randomMemberId();
        when( topologyService.coreTopologyForDatabase( NAMED_DATABASE_ID ) ).thenReturn(
                new DatabaseCoreTopology( DATABASE_ID, RaftId.from( DATABASE_ID ), Map.of( memberId, mock( CoreServerInfo.class ) ) ) );

        var shouldNotUse = spy( new AnotherDummyUpstreamDatabaseSelectionStrategy() );
        var shouldUse = spy( new AnotherDummyUpstreamDatabaseSelectionStrategy() );

        var selector = new UpstreamDatabaseStrategySelector( shouldNotUse, iterable( shouldUse ), NullLogProvider.getInstance() );

        // when
        selector.bestUpstreamMemberForDatabase( NAMED_DATABASE_ID );
        selector.bestUpstreamMembersForDatabase( NAMED_DATABASE_ID );

        // then
        verifyNoInteractions( shouldNotUse );
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
        public Optional<MemberId> upstreamMemberForDatabase( NamedDatabaseId namedDatabaseId )
        {
            return Optional.ofNullable( memberId );
        }

        @Override
        public Collection<MemberId> upstreamMembersForDatabase( NamedDatabaseId namedDatabaseId )
        {
            if ( memberId != null )
            {
                return List.of( memberId );
            }
            return List.of();
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
        public Optional<MemberId> upstreamMemberForDatabase( NamedDatabaseId namedDatabaseId )
        {
            return Optional.of( IdFactory.randomMemberId() );
        }

        @Override
        public Collection<MemberId> upstreamMembersForDatabase( NamedDatabaseId namedDatabaseId )
        {
            return List.of( IdFactory.randomMemberId() );
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
        public Optional<MemberId> upstreamMemberForDatabase( NamedDatabaseId namedDatabaseId )
        {
            return Optional.of( IdFactory.randomMemberId() );
        }

        @Override
        public Collection<MemberId> upstreamMembersForDatabase( NamedDatabaseId namedDatabaseId )
        {
            return List.of( IdFactory.randomMemberId() );
        }
    }
}
