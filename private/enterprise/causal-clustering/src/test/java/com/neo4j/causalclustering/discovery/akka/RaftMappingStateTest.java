/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.discovery.ReplicatedRaftMapping;
import com.neo4j.causalclustering.identity.CoreServerIdentity;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.InMemoryCoreServerIdentity;
import com.neo4j.causalclustering.identity.RaftMemberId;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.AssertableLogProvider;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.logging.AssertableLogProvider.Level.DEBUG;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.LogAssertions.assertThat;

class RaftMappingStateTest
{
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final RaftMappingState state = new RaftMappingState( logProvider.getLog( GlobalTopologyState.class ) );

    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final NamedDatabaseId namedDatabaseId1 = databaseIdRepository.getRaw( "db1" );
    private final DatabaseId databaseId1 = namedDatabaseId1.databaseId();
    private final NamedDatabaseId namedDatabaseId2 = databaseIdRepository.getRaw( "db2" );
    private final DatabaseId databaseId2 = namedDatabaseId2.databaseId();

    private final CoreServerIdentity myIdentity1 = new InMemoryCoreServerIdentity( 0 );
    private final CoreServerIdentity myIdentity2 = new InMemoryCoreServerIdentity( 1 );

    private final ServerId coreId1 = myIdentity1.serverId();
    private final ServerId coreId2 = myIdentity2.serverId();

    @Test
    void shouldWorkUpdateAdditions()
    {
        // given
        var memberId1 = myIdentity1.raftMemberId( namedDatabaseId1 );
        var memberId2 = myIdentity1.raftMemberId( namedDatabaseId2 );
        var mapping = ReplicatedRaftMapping.of( coreId1, Map.of( databaseId1, memberId1 ) );

        // when
        var changed = state.update( mapping );

        // then
        assertEquals( changed, Set.of( databaseId1 ) );
        assertEquals( memberId1, state.resolveRaftMemberForServer( databaseId1, coreId1 ) );
        assertEquals( coreId1, state.resolveServerForRaftMember( memberId1 ) );
        assertNull( state.resolveRaftMemberForServer( databaseId2, coreId1 ) );

        assertThat( logProvider ).forClass( GlobalTopologyState.class ).forLevel( INFO ).containsMessages(
                format( "Raft Mapping changed for server %s total number of mappings now is %d, nothing removed%n     added mappings %s",
                        coreId1, 1, Map.of( databaseId1, memberId1 ) ) );

        //given
        logProvider.clear();
        mapping = ReplicatedRaftMapping.of( coreId1, Map.of( databaseId1, memberId1, databaseId2, memberId2 ) );

        // when
        changed = state.update( mapping );

        // then
        assertEquals( changed, Set.of( databaseId2 ) );
        assertEquals( memberId1, state.resolveRaftMemberForServer( databaseId1, coreId1 ) );
        assertEquals( coreId1, state.resolveServerForRaftMember( memberId1 ) );
        assertEquals( memberId2, state.resolveRaftMemberForServer( databaseId2, coreId1 ) );
        assertEquals( coreId1, state.resolveServerForRaftMember( memberId2 ) );

        // fallback working for coreId2
        assertEquals( new RaftMemberId( coreId2.uuid() ), state.resolveRaftMemberForServer( databaseId1, coreId2 ) );
        assertEquals( new RaftMemberId( coreId2.uuid() ), state.resolveRaftMemberForServer( databaseId2, coreId2 ) );

        assertThat( logProvider ).forClass( GlobalTopologyState.class ).forLevel( INFO ).containsMessages(
                format( "Raft Mapping changed for server %s total number of mappings now is %d, nothing removed%n     added mappings %s",
                        coreId1, 2, Map.of( databaseId2, memberId2 ) ) );
    }

    @Test
    void shouldWorkUpdateRemoval()
    {
        // given
        var memberId1 = myIdentity1.raftMemberId( namedDatabaseId1 );
        var memberId2 = myIdentity1.raftMemberId( namedDatabaseId2 );
        var mapping = ReplicatedRaftMapping.of( coreId1, Map.of( databaseId1, memberId1, databaseId2, memberId2 ) );
        state.update( mapping );
        logProvider.clear();

        // when
        mapping = ReplicatedRaftMapping.of( coreId1, Map.of( databaseId1, memberId1 ) );
        var changed = state.update( mapping );

        // then
        assertEquals( changed, Set.of( databaseId2 ) );
        assertEquals( memberId1, state.resolveRaftMemberForServer( databaseId1, coreId1 ) );
        assertEquals( coreId1, state.resolveServerForRaftMember( memberId1 ) );
        assertNull( state.resolveRaftMemberForServer( databaseId2, coreId1 ) );
        // fallback working
        assertEquals( new ServerId( memberId2.uuid() ), state.resolveServerForRaftMember( memberId2 ) );

        assertThat( logProvider ).forClass( GlobalTopologyState.class ).forLevel( INFO ).containsMessages(
                format( "Raft Mapping changed for server %s total number of mappings now is %d, nothing added%n   removed mappings %s",
                        coreId1, 1, Set.of( databaseId2 ) ) );

        // when
        changed = state.update( ReplicatedRaftMapping.emptyOf( coreId1 ) );

        // then
        assertEquals( changed, Set.of( databaseId1 ) );
        // fallback won't work state has seen coreId1 already providing mappings
        assertNull( state.resolveRaftMemberForServer( databaseId1, coreId1 ) );
        assertNull( state.resolveRaftMemberForServer( databaseId2, coreId1 ) );
        // fallback works
        assertEquals( new ServerId( memberId1.uuid() ), state.resolveServerForRaftMember( memberId1 ) );
        assertEquals( new ServerId( memberId2.uuid() ), state.resolveServerForRaftMember( memberId2 ) );

        assertThat( logProvider ).forClass( GlobalTopologyState.class ).forLevel( INFO ).containsMessages(
                format( "Raft Mapping changed for server %s total number of mappings now is %d, nothing added%n   removed mappings %s",
                        coreId1, 0, Set.of( databaseId1 ) ) );
    }

    @Test
    void shouldWorkForMultiServer()
    {
        // given
        var memberId11 = myIdentity1.raftMemberId( namedDatabaseId1 );
        var memberId12 = myIdentity1.raftMemberId( namedDatabaseId2 );
        var memberId21 = myIdentity2.raftMemberId( namedDatabaseId1 );
        var memberId22 = myIdentity2.raftMemberId( namedDatabaseId2 );

        // when
        var changed1 = state.update( ReplicatedRaftMapping.of( coreId1, Map.of( databaseId1, memberId11, databaseId2, memberId12 ) ) );
        var changed2 = state.update( ReplicatedRaftMapping.of( coreId2, Map.of( databaseId1, memberId21, databaseId2, memberId22 ) ) );

        // then
        assertEquals( changed1, Set.of( databaseId1, databaseId2 ) );
        assertEquals( changed2, Set.of( databaseId1, databaseId2 ) );
        assertEquals( memberId11, state.resolveRaftMemberForServer( databaseId1, coreId1 ) );
        assertEquals( coreId1, state.resolveServerForRaftMember( memberId11 ) );
        assertEquals( memberId12, state.resolveRaftMemberForServer( databaseId2, coreId1 ) );
        assertEquals( coreId1, state.resolveServerForRaftMember( memberId12 ) );
        assertEquals( memberId21, state.resolveRaftMemberForServer( databaseId1, coreId2 ) );
        assertEquals( coreId2, state.resolveServerForRaftMember( memberId21 ) );
        assertEquals( memberId22, state.resolveRaftMemberForServer( databaseId2, coreId2 ) );
        assertEquals( coreId2, state.resolveServerForRaftMember( memberId22 ) );
    }

    @Test
    void emptyOrSameMappingShouldNotLog()
    {
        // given
        var memberId1 = myIdentity1.raftMemberId( namedDatabaseId1 );
        var memberId2 = myIdentity1.raftMemberId( namedDatabaseId2 );
        var mapping = ReplicatedRaftMapping.of( coreId1, Map.of( databaseId1, memberId1, databaseId2, memberId2 ) );
        state.update( mapping );
        logProvider.clear();

        // when
        mapping = ReplicatedRaftMapping.of( coreId1, Map.of( databaseId1, memberId1, databaseId2, memberId2 ) );
        var changed = state.update( mapping );

        // then
        assertThat( changed ).isEmpty();
        assertThat( logProvider ).doesNotHaveAnyLogs();

        // when
        changed = state.update( ReplicatedRaftMapping.emptyOf( IdFactory.randomServerId() ) );

        // then
        assertThat( changed ).isEmpty();
        assertThat( logProvider ).doesNotHaveAnyLogs();
    }

    @Test
    void shouldWorkRemoveAndAddTheSameTime()
    {
        // given
        var namedDatabaseId3 = databaseIdRepository.getRaw( "db3" );
        var databaseId3 = namedDatabaseId3.databaseId();
        var memberId1 = myIdentity1.raftMemberId( namedDatabaseId1 );
        var memberId2 = myIdentity1.raftMemberId( namedDatabaseId2 );
        var memberId3 = myIdentity1.raftMemberId( namedDatabaseId3 );
        state.update( ReplicatedRaftMapping.of( coreId1, Map.of( databaseId1, memberId1, databaseId2, memberId2 ) ) );
        logProvider.clear();

        // when
        var changed = state.update( ReplicatedRaftMapping.of( coreId1, Map.of( databaseId2, memberId2, databaseId3, memberId3 ) ) );

        // then
        assertEquals( changed, Set.of( databaseId1, databaseId3 ) );
        assertNull( state.resolveRaftMemberForServer( databaseId1, coreId1 ) );
        //assertNull( state.resolveServerForRaftMember( memberId1 ) );
        assertEquals( memberId2, state.resolveRaftMemberForServer( databaseId2, coreId1 ) );
        assertEquals( coreId1, state.resolveServerForRaftMember( memberId2 ) );
        assertEquals( memberId3, state.resolveRaftMemberForServer( databaseId3, coreId1 ) );
        assertEquals( coreId1, state.resolveServerForRaftMember( memberId3 ) );

        assertThat( logProvider ).forClass( GlobalTopologyState.class ).forLevel( INFO ).containsMessages(
                format( "Raft Mapping changed for server %s total number of mappings now is %d%n   removed mappings %s" +
                        "%n     added mappings %s", coreId1, 2, Set.of( databaseId1 ), Map.of( databaseId3, memberId3 ) ) );
    }
}
