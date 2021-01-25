/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.causalclustering.discovery.FakeTopologyService;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterServerInfosProviderTest
{
    @Test
    void shouldReflectTopologyState()
    {
        var dbFoo = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var dbBar = DatabaseIdFactory.from( "bar", UUID.randomUUID() );

        var coreFoo = new ServerId( UUID.randomUUID() );
        var rrFoo = new ServerId( UUID.randomUUID() );

        var coreBar = new ServerId( UUID.randomUUID() );
        var rrBar = new ServerId( UUID.randomUUID() );

        var fakeTopologyService = new FakeTopologyService( Set.of( coreBar, coreFoo ), Set.of( rrBar, rrFoo ), coreBar, Set.of() );
        fakeTopologyService.setDatabases( Set.of( coreFoo, rrFoo ), Set.of( dbFoo.databaseId() ) );
        // topology service leader should not be relevant to the ClusterServerInfos
        fakeTopologyService.setRole( coreFoo, RoleInfo.LEADER );
        fakeTopologyService.setDatabases( Set.of( coreBar, rrBar ), Set.of( dbBar.databaseId() ) );
        fakeTopologyService.setState( Set.of( coreBar, rrBar ), DiscoveryDatabaseState.unknown( dbBar.databaseId() ) );
        fakeTopologyService.setState( Set.of( coreFoo, rrFoo ), DiscoveryDatabaseState.unknown( dbFoo.databaseId() ) );

        var clusterServerInfosProvider = new ClusterServerInfosProvider( fakeTopologyService, new NoLeaderService() );

        var barResult = clusterServerInfosProvider.apply( dbBar );
        assertThat( barResult.leader().allServers() ).isEmpty();
        assertThat( barResult.cores().allServers().stream().map( ServerInfo::serverId ) ).containsExactly( coreBar );
        assertThat( barResult.readReplicas().allServers().stream().map( ServerInfo::serverId ) ).containsExactly( rrBar );

        var fooResult = clusterServerInfosProvider.apply( dbFoo );
        assertThat( fooResult.leader().allServers() ).isEmpty();
        assertThat( fooResult.cores().allServers().stream().map( ServerInfo::serverId ) ).containsExactly( coreFoo );
        assertThat( fooResult.readReplicas().allServers().stream().map( ServerInfo::serverId ) ).containsExactly( rrFoo );
    }

    private static class NoLeaderService implements LeaderService
    {
        @Override
        public Optional<ServerId> getLeaderId( NamedDatabaseId namedDatabaseId )
        {
            return Optional.empty();
        }

        @Override
        public Optional<SocketAddress> getLeaderBoltAddress( NamedDatabaseId namedDatabaseId )
        {
            return Optional.empty();
        }
    }
}
