/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftMemberId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;

import static com.neo4j.server.rest.causalclustering.ClusteringDatabaseStatusUtil.coreStatusMockBuilder;
import static com.neo4j.server.rest.causalclustering.ClusteringDatabaseStatusUtil.readReplicaStatusMockBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofMillis;
import static java.util.Comparator.naturalOrder;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ClusteringDbmsStatusTest
{
    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final DatabaseManagementService managementService = mock( DatabaseManagementService.class );
    private final ClusteringDbmsStatus statusOutput = new ClusteringDbmsStatus( managementService );

    private final RaftMemberId coreId1 = RaftMemberId.of( randomUUID() );
    private final RaftMemberId coreId2 = RaftMemberId.of( randomUUID() );
    private final MemberId readReplicaId = MemberId.of( randomUUID() );

    private final GraphDatabaseAPI coreDb1 = coreStatusMockBuilder()
            .databaseId( databaseIdRepository.getRaw( "foo" ) )
            .memberId( coreId1 )
            .leaderId( coreId2 )
            .healthy( true )
            .available( true )
            .durationSinceLastMessage( ofMillis( 1 ) )
            .appliedCommandIndex( 2 )
            .throughput( 3 )
            .role( Role.LEADER )
            .build();

    private final GraphDatabaseAPI coreDb2 = coreStatusMockBuilder()
            .databaseId( databaseIdRepository.getRaw( "bar" ) )
            .memberId( coreId2 )
            .leaderId( coreId1 )
            .healthy( true )
            .available( true )
            .durationSinceLastMessage( ofMillis( 4 ) )
            .appliedCommandIndex( 5 )
            .throughput( 6 )
            .role( Role.FOLLOWER )
            .build();

    private final GraphDatabaseAPI readReplicaDb = readReplicaStatusMockBuilder()
            .databaseId( databaseIdRepository.getRaw( "baz" ) )
            .healthy( false )
            .available( true )
            .readReplicaId( readReplicaId )
            .coreRole( MemberId.of( coreId1 ), RoleInfo.FOLLOWER )
            .coreRole( MemberId.of( coreId2 ), RoleInfo.LEADER )
            .appliedCommandIndex( 7 )
            .throughput( 8 )
            .build();

    @BeforeEach
    void beforeEach()
    {
        var databaseNames = List.of( coreDb1.databaseName(), coreDb2.databaseName(), readReplicaDb.databaseName() );
        when( managementService.listDatabases() ).thenReturn( databaseNames );

        when( managementService.database( coreDb1.databaseName() ) ).thenReturn( coreDb1 );
        when( managementService.database( coreDb2.databaseName() ) ).thenReturn( coreDb2 );
        when( managementService.database( readReplicaDb.databaseName() ) ).thenReturn( readReplicaDb );
    }

    @Test
    void shouldOutputDatabaseName() throws Exception
    {
        var json = produceJson();

        verifyDatabaseNames( json, coreDb1, coreDb2, readReplicaDb );
    }

    @Test
    void shouldOutputDatabaseUuid() throws Exception
    {
        var json = produceJson();

        verifyDatabaseUuids( json, coreDb1, coreDb2, readReplicaDb );
    }

    @Test
    void shouldOutputDatabaseStatus() throws Exception
    {
        var json = produceJson();

        var status1 = findDatabaseStatus( json, coreDb1 );
        verifyLastAppliedRaftIndex( status1, 2 );
        verifyParticipatingInRaftGroup( status1, true );
        verifyVotingMembers( status1, coreId1, coreId2 );
        verifyHealthy( status1, true );
        verifyMemberId( status1, coreId1.getUuid() );
        verifyLeader( status1, coreId2 );
        verifyMillisSinceLastLeaderMessage( status1, 1 );
        verifyRaftCommandsPerSecond( status1, 3.0 );
        verifyCore( status1, true );

        var status2 = findDatabaseStatus( json, coreDb2 );
        verifyLastAppliedRaftIndex( status2, 5 );
        verifyParticipatingInRaftGroup( status2, true );
        verifyVotingMembers( status2, coreId1, coreId2 );
        verifyHealthy( status2, true );
        verifyMemberId( status2, coreId2.getUuid() );
        verifyLeader( status2, coreId1 );
        verifyMillisSinceLastLeaderMessage( status2, 4 );
        verifyRaftCommandsPerSecond( status2, 6.0 );
        verifyCore( status2, true );

        var status3 = findDatabaseStatus( json, readReplicaDb );
        verifyLastAppliedRaftIndex( status3, 7 );
        verifyParticipatingInRaftGroup( status3, false );
        verifyVotingMembers( status3, coreId1, coreId2 );
        verifyHealthy( status3, false );
        verifyMemberId( status3, readReplicaId.getUuid() );
        verifyLeader( status3, coreId2 );
        verifyMillisSinceLastLeaderMessage( status3, null );
        verifyRaftCommandsPerSecond( status3, 8.0 );
        verifyCore( status3, false );
    }

    @Test
    void shouldSkipUnavailableDatabase() throws Exception
    {
        when( coreDb2.isAvailable( anyLong() ) ).thenReturn( false );

        var json = produceJson();

        verifyDatabaseNames( json, coreDb1, readReplicaDb );
        verifyDatabaseUuids( json, coreDb1, readReplicaDb );
    }

    @Test
    void shouldSkipNotFoundDatabase() throws Exception
    {
        when( managementService.database( coreDb2.databaseName() ) ).thenThrow( DatabaseNotFoundException.class );

        var json = produceJson();

        verifyDatabaseNames( json, coreDb1, readReplicaDb );
        verifyDatabaseUuids( json, coreDb1, readReplicaDb );
    }

    @Test
    void shouldFailForDatabaseWithIllegalType()
    {
        var illegalInfos = Stream.of( DbmsInfo.values() )
                                 .filter( info -> info != DbmsInfo.CORE && info != DbmsInfo.READ_REPLICA )
                                 .collect( toList() );

        for ( var illegalInfo : illegalInfos )
        {
            var db = coreStatusMockBuilder()
                    .databaseId( databaseIdRepository.getRaw( "qux" ) )
                    .memberId( coreId1 )
                    .leaderId( coreId2 )
                    .available( true )
                    .build();

            when( db.dbmsInfo() ).thenReturn( illegalInfo );
            when( managementService.listDatabases() ).thenReturn( List.of( "qux" ) );
            when( managementService.database( "qux" ) ).thenReturn( db );

            assertThrows( IllegalStateException.class, this::produceJson );
        }
    }

    private List<Map<String,Object>> produceJson() throws IOException, JsonParseException
    {
        var outputStream = new ByteArrayOutputStream();
        statusOutput.write( outputStream );
        var jsonString = new String( outputStream.toByteArray(), UTF_8 );
        return JsonHelper.jsonToList( jsonString );
    }

    private static void verifyDatabaseNames( List<Map<String,Object>> json, GraphDatabaseAPI... dbs )
    {
        var expectedDatabaseNames = Stream.of( dbs )
                                          .map( db -> db.databaseId().name() )
                                          .collect( toSet() );

        var actualDatabaseNames = json.stream()
                                      .map( element -> (String) element.get( "databaseName" ) )
                                      .collect( toSet() );

        assertEquals( expectedDatabaseNames, actualDatabaseNames );
    }

    private static void verifyDatabaseUuids( List<Map<String,Object>> json, GraphDatabaseAPI... dbs )
    {
        var expectedDatabaseUuids = Stream.of( dbs )
                                          .map( db -> db.databaseId().databaseId().uuid().toString() )
                                          .collect( toSet() );

        var actualDatabaseNames = json.stream()
                                      .map( element -> (String) element.get( "databaseUuid" ) )
                                      .collect( toSet() );

        assertEquals( expectedDatabaseUuids, actualDatabaseNames );
    }

    @SuppressWarnings( "unchecked" )
    private static Map<String,Object> findDatabaseStatus( List<Map<String,Object>> json, GraphDatabaseAPI db )
    {
        return json.stream()
                   .filter( element -> db.databaseName().equals( element.get( "databaseName" ) ) )
                   .map( element -> (Map<String,Object>) element.get( "databaseStatus" ) )
                   .findFirst()
                   .orElseThrow();
    }

    private static void verifyLastAppliedRaftIndex( Map<String,Object> status, int expected )
    {
        var value = status.get( "lastAppliedRaftIndex" );
        assertThat( value, instanceOf( Integer.class ) );
        assertEquals( expected, status.get( "lastAppliedRaftIndex" ) );
    }

    private static void verifyParticipatingInRaftGroup( Map<String,Object> status, boolean expectedValue )
    {
        var value = status.get( "participatingInRaftGroup" );
        assertThat( value, instanceOf( Boolean.class ) );
        assertEquals( expectedValue, value );
    }

    @SuppressWarnings( "unchecked" )
    private static void verifyVotingMembers( Map<String,Object> status, RaftMemberId... expected )
    {
        var value = status.get( "votingMembers" );
        assertThat( value, instanceOf( List.class ) );

        var expectedVotingMember = Stream.of( expected ).map( id -> id.getUuid().toString() ).sorted().collect( toList() );
        var actualVotingMember = (List<String>) status.get( "votingMembers" );
        actualVotingMember.sort( naturalOrder() );

        assertEquals( expectedVotingMember, actualVotingMember );
    }

    private static void verifyHealthy( Map<String,Object> status, boolean expected )
    {
        var value = status.get( "healthy" );
        assertThat( value, instanceOf( Boolean.class ) );
        assertEquals( expected, value );
    }

    private static void verifyMemberId( Map<String,Object> status, UUID expected )
    {
        var value = status.get( "memberId" );
        assertThat( value, instanceOf( String.class ) );
        assertEquals( expected.toString(), value );
    }

    private static void verifyLeader( Map<String,Object> status, RaftMemberId expected )
    {
        var value = status.get( "leader" );
        assertThat( value, instanceOf( String.class ) );
        assertEquals( expected.getUuid().toString(), value );
    }

    private static void verifyMillisSinceLastLeaderMessage( Map<String,Object> status, Integer expected )
    {
        var value = status.get( "millisSinceLastLeaderMessage" );
        assertEquals( expected, value );
    }

    private static void verifyRaftCommandsPerSecond( Map<String,Object> status, Double expected )
    {
        var value = status.get( "raftCommandsPerSecond" );
        if ( expected == null )
        {
            assertNull( value );
        }
        else
        {
            assertThat( value, instanceOf( Double.class ) );
            assertEquals( expected, value );
        }
    }

    private static void verifyCore( Map<String,Object> status, boolean expected )
    {
        var value = status.get( "core" );
        assertThat( value, instanceOf( Boolean.class ) );
        assertEquals( expected, value );
    }
}