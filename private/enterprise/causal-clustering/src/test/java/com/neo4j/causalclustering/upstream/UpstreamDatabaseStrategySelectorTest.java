/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream;

import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.upstream.strategies.ConnectToRandomCoreServerStrategy;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.identity.ServerId;
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
        ServerId theServerId = IdFactory.randomServerId();
        goodOne.setServerId( theServerId );

        UpstreamDatabaseStrategySelector selector =
                new UpstreamDatabaseStrategySelector( badOne, iterable( goodOne, anotherBadOne ), NullLogProvider.getInstance() );

        // when
        ServerId result = selector.bestUpstreamServerForDatabase( NAMED_DATABASE_ID );
        Collection<ServerId> results = selector.bestUpstreamServersForDatabase( NAMED_DATABASE_ID );

        // then
        assertEquals( List.of( theServerId ), results );
        assertEquals( theServerId, result );
    }

    @Test
    void shouldDefaultToRandomCoreServerIfNoOtherStrategySpecified() throws Exception
    {
        // given
        TopologyService topologyService = mock( TopologyService.class );
        ServerId serverId = IdFactory.randomServerId();
        when( topologyService.coreTopologyForDatabase( NAMED_DATABASE_ID ) )
                .thenReturn( new DatabaseCoreTopology( DATABASE_ID, RaftGroupId.from( DATABASE_ID ), Map.of( serverId, mock( CoreServerInfo.class ) ) ) );

        ConnectToRandomCoreServerStrategy defaultStrategy = new ConnectToRandomCoreServerStrategy();
        defaultStrategy.inject( topologyService, Config.defaults(), NullLogProvider.getInstance(), null );

        UpstreamDatabaseStrategySelector selector = new UpstreamDatabaseStrategySelector( defaultStrategy );

        // when
        ServerId instance = selector.bestUpstreamServerForDatabase( NAMED_DATABASE_ID );
        Collection<ServerId> instances = selector.bestUpstreamServersForDatabase( NAMED_DATABASE_ID );

        // then
        assertEquals( serverId, instance );
        assertEquals( List.of( serverId ), instances );
    }

    @Test
    void shouldUseSpecifiedStrategyInPreferenceToDefault() throws Exception
    {
        // given
        TopologyService topologyService = mock( TopologyService.class );
        ServerId serverId = IdFactory.randomServerId();
        when( topologyService.coreTopologyForDatabase( NAMED_DATABASE_ID ) ).thenReturn(
                new DatabaseCoreTopology( DATABASE_ID, RaftGroupId.from( DATABASE_ID ), Map.of( serverId, mock( CoreServerInfo.class ) ) ) );

        var shouldNotUse = spy( new AnotherDummyUpstreamDatabaseSelectionStrategy() );
        var shouldUse = spy( new AnotherDummyUpstreamDatabaseSelectionStrategy() );

        var selector = new UpstreamDatabaseStrategySelector( shouldNotUse, iterable( shouldUse ), NullLogProvider.getInstance() );

        // when
        selector.bestUpstreamServerForDatabase( NAMED_DATABASE_ID );
        selector.bestUpstreamServersForDatabase( NAMED_DATABASE_ID );

        // then
        verifyNoInteractions( shouldNotUse );
    }

    @ServiceProvider
    public static class DummyUpstreamDatabaseSelectionStrategy extends UpstreamDatabaseSelectionStrategy
    {
        private ServerId serverId;

        public DummyUpstreamDatabaseSelectionStrategy()
        {
            super( "dummy" );
        }

        @Override
        public Optional<ServerId> upstreamServerForDatabase( NamedDatabaseId namedDatabaseId )
        {
            return Optional.ofNullable( serverId );
        }

        @Override
        public Collection<ServerId> upstreamServersForDatabase( NamedDatabaseId namedDatabaseId )
        {
            if ( serverId != null )
            {
                return List.of( serverId );
            }
            return List.of();
        }

        public void setServerId( ServerId serverId )
        {
            this.serverId = serverId;
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
        public Optional<ServerId> upstreamServerForDatabase( NamedDatabaseId namedDatabaseId )
        {
            return Optional.of( IdFactory.randomServerId() );
        }

        @Override
        public Collection<ServerId> upstreamServersForDatabase( NamedDatabaseId namedDatabaseId )
        {
            return List.of( IdFactory.randomServerId() );
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
        public Optional<ServerId> upstreamServerForDatabase( NamedDatabaseId namedDatabaseId )
        {
            return Optional.of( IdFactory.randomServerId() );
        }

        @Override
        public Collection<ServerId> upstreamServersForDatabase( NamedDatabaseId namedDatabaseId )
        {
            return List.of( IdFactory.randomServerId() );
        }
    }
}
