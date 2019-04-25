/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.identity.MemberId;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.VirtualValues;

import static com.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static com.neo4j.causalclustering.discovery.TestTopology.addressesForReadReplica;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.values.storable.Values.stringValue;

class ClusterOverviewProcedureTest
{
    @Test
    void shouldProvideOverviewOfCoreServersAndReadReplicas() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        MemberId theLeader = new MemberId( UUID.randomUUID() );
        MemberId follower1 = new MemberId( UUID.randomUUID() );
        MemberId follower2 = new MemberId( UUID.randomUUID() );

        Set<DatabaseId> leaderDatabases = databaseIds( "customers", "orders", "system" );
        Set<DatabaseId> follower1Databases = databaseIds( "system", "orders" );
        Set<DatabaseId> follower2Databases = databaseIds( "system" );
        coreMembers.put( theLeader, addressesForCore( 0, false, leaderDatabases ) );
        coreMembers.put( follower1, addressesForCore( 1, false, follower1Databases ) );
        coreMembers.put( follower2, addressesForCore( 2, false, follower2Databases ) );

        Map<MemberId,ReadReplicaInfo> replicaMembers = new HashMap<>();
        MemberId replica4 = new MemberId( UUID.randomUUID() );
        MemberId replica5 = new MemberId( UUID.randomUUID() );

        Set<DatabaseId> replica1Databases = databaseIds( "system", "orders" );
        Set<DatabaseId> replica2Databases = databaseIds( "system", "customers" );
        replicaMembers.put( replica4, addressesForReadReplica( 4, replica1Databases ) );
        replicaMembers.put( replica5, addressesForReadReplica( 5, replica2Databases ) );

        Map<MemberId,RoleInfo> roleMap = new HashMap<>();
        roleMap.put( theLeader, RoleInfo.LEADER );
        roleMap.put( follower1, RoleInfo.FOLLOWER );
        roleMap.put( follower2, RoleInfo.FOLLOWER );

        when( topologyService.allCoreServers() ).thenReturn( coreMembers );
        when( topologyService.allReadReplicas() ).thenReturn( replicaMembers );
        when( topologyService.allCoreRoles() ).thenReturn( roleMap );

        ClusterOverviewProcedure procedure =
                new ClusterOverviewProcedure( topologyService, NullLogProvider.getInstance() );

        // when
        final RawIterator<AnyValue[],ProcedureException> members = procedure.apply( null, new AnyValue[0], null );

        assertThat( members.next(), new IsRecord( theLeader.getUuid(), 5000, RoleInfo.LEADER, Set.of( "core", "core0" ), leaderDatabases ) );
        assertThat( members.next(),
                new IsRecord( follower1.getUuid(), 5001, RoleInfo.FOLLOWER, Set.of( "core", "core1" ), follower1Databases ) );
        assertThat( members.next(),
                new IsRecord( follower2.getUuid(), 5002, RoleInfo.FOLLOWER, Set.of( "core", "core2" ), follower2Databases ) );

        assertThat( members.next(),
                new IsRecord( replica4.getUuid(), 6004, RoleInfo.READ_REPLICA, Set.of( "replica", "replica4" ), replica1Databases ) );
        assertThat( members.next(),
                new IsRecord( replica5.getUuid(), 6005, RoleInfo.READ_REPLICA, Set.of( "replica", "replica5" ), replica2Databases ) );

        assertFalse( members.hasNext() );
    }

    private static Set<DatabaseId> databaseIds( String... names )
    {
        return Arrays.stream( names )
                .map( DatabaseId::new )
                .collect( toSet() );
    }

    private static class IsRecord extends TypeSafeMatcher<AnyValue[]>
    {
        private final UUID memberId;
        private final int boltPort;
        private final RoleInfo role;
        private final Set<String> groups;
        private final Set<DatabaseId> databaseIds;

        IsRecord( UUID memberId, int boltPort, RoleInfo role, Set<String> groups, Set<DatabaseId> databaseIds )
        {
            this.memberId = memberId;
            this.boltPort = boltPort;
            this.role = role;
            this.groups = groups;
            this.databaseIds = databaseIds;
        }

        @Override
        protected boolean matchesSafely( AnyValue[] record )
        {
            if ( record.length != 5 )
            {
                return false;
            }

            if ( !stringValue( memberId.toString() ).equals( record[0] ) )
            {
                return false;
            }

            ListValue boltAddresses = VirtualValues.list( stringValue( "bolt://localhost:" + boltPort ) );

            if ( !boltAddresses.equals( record[1] ) )
            {
                return false;
            }

            if ( !stringValue( role.name() ).equals( record[2] ) )
            {
                return false;
            }

            Set<String> recordGroups = new HashSet<>();
            for ( AnyValue value : (ListValue) record[3] )
            {
                recordGroups.add( ((TextValue) value).stringValue() );
            }
            if ( !groups.equals( recordGroups ) )
            {
                return false;
            }

            Set<DatabaseId> recordDatabaseNames = new HashSet<>();
            for ( AnyValue value : (ListValue) record[4] )
            {
                recordDatabaseNames.add( new DatabaseId( ((TextValue) value).stringValue() ) );
            }
            if ( !databaseIds.equals( recordDatabaseNames ) )
            {
                return false;
            }

            return true;
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText(
                    "memberId=" + memberId +
                    ", boltPort=" + boltPort +
                    ", role=" + role +
                    ", groups=" + groups +
                    ", databaseIds=" + databaseIds +
                    '}' );
        }
    }
}
