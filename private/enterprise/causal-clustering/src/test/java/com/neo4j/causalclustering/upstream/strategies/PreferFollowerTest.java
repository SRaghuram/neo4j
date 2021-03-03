/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.NullLogProvider;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PreferFollowerTest
{
    private final NamedDatabaseId databaseId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
    private final Config config = Config.emptyBuilder().build();

    final Map<ServerId,RoleInfo> serverIdRoleMap = Map.of( new ServerId( UUID.randomUUID() ), RoleInfo.FOLLOWER,
                                                           new ServerId( UUID.randomUUID() ), RoleInfo.LEADER,
                                                           new ServerId( UUID.randomUUID() ), RoleInfo.READ_REPLICA,
                                                           new ServerId( UUID.randomUUID() ), RoleInfo.UNKNOWN );

    @Test
    void followerShouldHaveHighestPriority() throws UpstreamDatabaseSelectionException
    {
        final var strategy = new PreferFollower();
        final var topologyService = createTopologyService( serverIdRoleMap, List.of(), databaseId );
        strategy.inject( topologyService, config, NullLogProvider.getInstance(), new ServerId( UUID.randomUUID() ) );

        // when
        final var selectedServerId = strategy.upstreamServerForDatabase( databaseId );

        // then
        assertThat( findServerIdWithRole( RoleInfo.FOLLOWER, serverIdRoleMap ) ).isEqualTo( selectedServerId );
    }

    @Test
    void leaderShouldHaveHighestPriorityWhenThereAreNoFollowers() throws UpstreamDatabaseSelectionException
    {
        final var strategy = new PreferFollower();
        final var filterFromServerIdRoleMap = List.of( findServerIdWithRole( RoleInfo.FOLLOWER, serverIdRoleMap ).get() );
        final var topologyService = createTopologyService( serverIdRoleMap, filterFromServerIdRoleMap, databaseId );
        strategy.inject( topologyService, config, NullLogProvider.getInstance(), new ServerId( UUID.randomUUID() ) );

        // when
        final var selectedServerId = strategy.upstreamServerForDatabase( databaseId );

        // then
        assertThat( findServerIdWithRole( RoleInfo.LEADER, serverIdRoleMap ) ).isEqualTo( selectedServerId );
    }

    @Test
    void readReplicaShouldHaveHighestPriorityWhenThereAreNoFollowersAndLeader() throws UpstreamDatabaseSelectionException
    {
        final var strategy = new PreferFollower();
        final var filterFromServerIdRoleMap = List.of( findServerIdWithRole( RoleInfo.FOLLOWER, serverIdRoleMap ).get(),
                                                       findServerIdWithRole( RoleInfo.LEADER, serverIdRoleMap ).get() );
        final var topologyService = createTopologyService( serverIdRoleMap, filterFromServerIdRoleMap, databaseId );
        strategy.inject( topologyService, config, NullLogProvider.getInstance(), new ServerId( UUID.randomUUID() ) );

        // when
        final var selectedServerId = strategy.upstreamServerForDatabase( databaseId );

        // then
        assertThat( findServerIdWithRole( RoleInfo.READ_REPLICA, serverIdRoleMap ) ).isEqualTo( selectedServerId );
    }

    @Test
    void memberWithUnknownRoleShouldBeReturnedIfThereAreNoOtherMembers() throws UpstreamDatabaseSelectionException
    {
        final var strategy = new PreferFollower();
        final var filterFromServerIdRoleMap = List.of( findServerIdWithRole( RoleInfo.FOLLOWER, serverIdRoleMap ).get(),
                                                       findServerIdWithRole( RoleInfo.LEADER, serverIdRoleMap ).get(),
                                                       findServerIdWithRole( RoleInfo.READ_REPLICA, serverIdRoleMap ).get() );
        final var topologyService = createTopologyService( serverIdRoleMap, filterFromServerIdRoleMap, databaseId );
        strategy.inject( topologyService, config, NullLogProvider.getInstance(), new ServerId( UUID.randomUUID() ) );

        // when
        final var selectedServerId = strategy.upstreamServerForDatabase( databaseId );

        // then
        assertThat( findServerIdWithRole( RoleInfo.UNKNOWN, serverIdRoleMap ) ).isEqualTo( selectedServerId );
    }

    private Optional<ServerId> findServerIdWithRole( RoleInfo roleInfo, Map<ServerId,RoleInfo> serverIdRoleMap )
    {
        return serverIdRoleMap.entrySet().stream().filter( entry -> entry.getValue() == roleInfo ).findFirst().map( Map.Entry::getKey );
    }

    private TopologyService createTopologyService( Map<ServerId,RoleInfo> serverIdRoleMap, List<ServerId> filterFromServerIdMap, NamedDatabaseId databaseId )
    {
        Map<ServerId,RoleInfo> serverIdRoleMapCopy = new HashMap<>( serverIdRoleMap );
        filterFromServerIdMap.forEach( serverIdRoleMapCopy::remove );

        final var topologyService = mock( TopologyService.class );
        final var databaseCoreTopology = mock( DatabaseCoreTopology.class );
        final var coreServerInfo = mock( CoreServerInfo.class );
        final var servers = serverIdRoleMapCopy.keySet().stream()
                                               .collect( Collectors.toMap( serverId -> serverId, serverId -> coreServerInfo ) );

        when( databaseCoreTopology.servers() ).thenReturn( servers );
        when( topologyService.coreTopologyForDatabase( databaseId ) ).thenReturn( databaseCoreTopology );
        serverIdRoleMapCopy.forEach( ( key, value ) -> when( topologyService.lookupRole( databaseId, key ) ).thenReturn( serverIdRoleMapCopy.get( key ) ) );

        return topologyService;
    }
}
