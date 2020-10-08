/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.member;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.identity.ClusteringIdentityModule;
import com.neo4j.causalclustering.identity.StubClusteringIdentityModule;
import com.neo4j.dbms.EnterpriseDatabaseState;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.StubDatabaseStateService;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static com.neo4j.dbms.EnterpriseOperatorState.DIRTY;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultDiscoveryMemberTest
{
    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final ClusteringIdentityModule identityModule = new StubClusteringIdentityModule();
    private final NamedDatabaseId databaseId1 = databaseIdRepository.getRaw( "one" );
    private final NamedDatabaseId databaseId2 = databaseIdRepository.getRaw( "two" );
    private final NamedDatabaseId databaseId3 = databaseIdRepository.getRaw( "three" );

    @Test
    void shouldCorrectlyFilterStoppedDatabases()
    {
        Map<NamedDatabaseId,DatabaseState> states = Map.of(
                databaseId1, new EnterpriseDatabaseState( databaseId1, STARTED ),
                databaseId2, new EnterpriseDatabaseState( databaseId2, STOPPED ),
                databaseId3, new EnterpriseDatabaseState( databaseId3, STARTED )
        );

        var databaseStates = new StubDatabaseStateService( states, EnterpriseDatabaseState::unknown );
        var discoveryMember = DefaultDiscoveryMember.factory( identityModule, databaseStates, Map.of() );

        assertEquals( Set.of( databaseId1.databaseId(), databaseId3.databaseId() ), discoveryMember.databasesInState( STARTED ) );
    }

    @Test
    void shouldCorrectlyFilterFailedDatabases()
    {
        var err = new IOException();
        Map<NamedDatabaseId,DatabaseState> states = Map.of(
                databaseId1, new EnterpriseDatabaseState( databaseId1, DIRTY ).failed( err ),
                databaseId2, new EnterpriseDatabaseState( databaseId2, STARTED ),
                databaseId3, new EnterpriseDatabaseState( databaseId3, STARTED )
        );

        var databaseStates = new StubDatabaseStateService( states, EnterpriseDatabaseState::unknown );
        var discoveryMember = DefaultDiscoveryMember.factory( identityModule, databaseStates, Map.of() );

        assertEquals( Set.of( databaseId2.databaseId(), databaseId3.databaseId() ), discoveryMember.databasesInState( STARTED ) );
    }

    @Test
    void discoveryMemberContentsShouldBeUnmodifiable()
    {
        var databaseStates = new StubDatabaseStateService( EnterpriseDatabaseState::unknown );
        var discoveryMember = DefaultDiscoveryMember.factory( identityModule, databaseStates, Map.of() );

        assertSame( discoveryMember.databaseStates(), discoveryMember.databaseStates() );
        assertThat( discoveryMember.databaseStates() ).isEmpty();
        assertThrows( UnsupportedOperationException.class,
                      () -> discoveryMember.databaseLeaderships().put( databaseId2.databaseId(), LeaderInfo.INITIAL ) );
        assertThrows( UnsupportedOperationException.class,
                      () -> discoveryMember.databaseMemberships().put( databaseId2.databaseId(), identityModule.memberId( databaseId2 ) ) );
        assertThrows( UnsupportedOperationException.class,
                      () -> discoveryMember.databaseStates().put( databaseId2.databaseId(), new EnterpriseDatabaseState( databaseId2, STARTED ) ) );
        assertThrows( UnsupportedOperationException.class,
                      () -> discoveryMember.databasesInState( STARTED ).add( databaseId1.databaseId() ) );
    }
}
