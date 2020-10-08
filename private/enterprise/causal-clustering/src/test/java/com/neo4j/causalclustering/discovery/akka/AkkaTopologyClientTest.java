/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.discovery.TestDiscoveryMember;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.StubClusteringIdentityModule;
import com.neo4j.dbms.EnterpriseDatabaseState;
import com.neo4j.dbms.EnterpriseOperatorState;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.StubDatabaseStateService;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.discovery.akka.GlobalTopologyStateTestUtil.setupCoreTopologyState;
import static com.neo4j.causalclustering.discovery.akka.GlobalTopologyStateTestUtil.setupReadReplicaTopologyState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

class AkkaTopologyClientTest
{
    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();

    @Test
    void shouldReportEmptyTopologiesWhenShutdown() throws Exception
    {

        var databaseId = databaseIdRepository.getRaw( "people" );
        var identityModule = new StubClusteringIdentityModule();
        var databaseStates = Map.<NamedDatabaseId,DatabaseState>of( databaseId, new EnterpriseDatabaseState( databaseId, EnterpriseOperatorState.STARTED ) );
        var databaseStateService = new StubDatabaseStateService( databaseStates, EnterpriseDatabaseState::unknown );
        var memberId1 = identityModule.memberId();
        var memberId2 = IdFactory.randomMemberId();
        var memberId3 = IdFactory.randomMemberId();

        var topologyClient = new AkkaTopologyClient( Config.defaults(), nullLogProvider(), identityModule,
                                                     mock( ActorSystemLifecycle.class, RETURNS_MOCKS ),
                                                     TestDiscoveryMember::factory,
                                                     Clocks.systemClock(), databaseStateService );

        topologyClient.init();
        topologyClient.start();

        // setup fake topology for cores
        setupCoreTopologyState( topologyClient.topologyState(), databaseId, memberId1, memberId2, memberId3 );

        // setup fake topology for read replicas
        setupReadReplicaTopologyState( topologyClient.topologyState(), databaseId, memberId1, memberId2 );

        // verify core topology is not empty
        assertEquals( Set.of( memberId1, memberId2, memberId3 ), topologyClient.coreTopologyForDatabase( databaseId ).servers().keySet() );
        assertEquals( Set.of( memberId1, memberId2, memberId3 ), topologyClient.allCoreServers().keySet() );

        // verify read replica topology is not empty
        assertEquals( Set.of( memberId1, memberId2 ), topologyClient.readReplicaTopologyForDatabase( databaseId ).servers().keySet() );
        assertEquals( Set.of( memberId1, memberId2 ), topologyClient.allReadReplicas().keySet() );

        topologyClient.stop();
        topologyClient.shutdown();

        // verify core topology is empty
        assertThat( topologyClient.coreTopologyForDatabase( databaseId ).servers().keySet(), is( empty() ) );
        assertThat( topologyClient.allCoreServers().keySet(), is( empty() ) );

        // verify read replica topology is empty
        assertThat( topologyClient.readReplicaTopologyForDatabase( databaseId ).servers().keySet(), is( empty() ) );
        assertThat( topologyClient.allCoreServers().keySet(), is( empty() ) );
    }
}
