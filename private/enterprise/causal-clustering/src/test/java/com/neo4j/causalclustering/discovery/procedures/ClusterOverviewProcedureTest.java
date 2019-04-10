/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopology;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.ReadReplicaTopology;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.identity.MemberId;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.VirtualValues;

import static com.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static com.neo4j.causalclustering.discovery.TestTopology.addressesForReadReplica;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.values.storable.Values.stringValue;

public class ClusterOverviewProcedureTest
{
    private static final String DATABASE_NAME = "customers";

    @Test
    public void shouldProvideOverviewOfCoreServersAndReadReplicas() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        MemberId theLeader = new MemberId( UUID.randomUUID() );
        MemberId follower1 = new MemberId( UUID.randomUUID() );
        MemberId follower2 = new MemberId( UUID.randomUUID() );

        coreMembers.put( theLeader, addressesForCore( 0, false ) );
        coreMembers.put( follower1, addressesForCore( 1, false ) );
        coreMembers.put( follower2, addressesForCore( 2, false ) );

        Map<MemberId,ReadReplicaInfo> replicaMembers = new HashMap<>();
        MemberId replica4 = new MemberId( UUID.randomUUID() );
        MemberId replica5 = new MemberId( UUID.randomUUID() );

        replicaMembers.put( replica4, addressesForReadReplica( 4 ) );
        replicaMembers.put( replica5, addressesForReadReplica( 5 ) );

        Map<MemberId,RoleInfo> roleMap = new HashMap<>();
        roleMap.put( theLeader, RoleInfo.LEADER );
        roleMap.put( follower1, RoleInfo.FOLLOWER );
        roleMap.put( follower2, RoleInfo.FOLLOWER );

        when( topologyService.coreServersForDatabase( DATABASE_NAME ) ).thenReturn( new CoreTopology( DATABASE_NAME, null, false, coreMembers ) );
        when( topologyService.readReplicasForDatabase( DATABASE_NAME ) ).thenReturn( new ReadReplicaTopology( DATABASE_NAME, replicaMembers ) );
        when( topologyService.allCoreRoles() ).thenReturn( roleMap );

        ClusterOverviewProcedure procedure =
                new ClusterOverviewProcedure( topologyService, NullLogProvider.getInstance() );

        // when
        final RawIterator<AnyValue[],ProcedureException> members = procedure.apply( null, new AnyValue[0], null );

        // todo: test with multiple databases
        assertThat( members.next(), new IsRecord( theLeader.getUuid(), 5000, RoleInfo.LEADER, Set.of( "core", "core0" ), Set.of( DATABASE_NAME ) ) );
        assertThat( members.next(),
                new IsRecord( follower1.getUuid(), 5001, RoleInfo.FOLLOWER, Set.of( "core", "core1" ), Set.of( DATABASE_NAME ) ) );
        assertThat( members.next(),
                new IsRecord( follower2.getUuid(), 5002, RoleInfo.FOLLOWER, Set.of( "core", "core2" ), Set.of( DATABASE_NAME ) ) );

        assertThat( members.next(),
                new IsRecord( replica4.getUuid(), 6004, RoleInfo.READ_REPLICA, Set.of( "replica", "replica4" ), Set.of( DATABASE_NAME ) ) );
        assertThat( members.next(),
                new IsRecord( replica5.getUuid(), 6005, RoleInfo.READ_REPLICA, Set.of( "replica", "replica5" ), Set.of( DATABASE_NAME ) ) );

        assertFalse( members.hasNext() );
    }

    class IsRecord extends TypeSafeMatcher<AnyValue[]>
    {
        private final UUID memberId;
        private final int boltPort;
        private final RoleInfo role;
        private final Set<String> groups;
        private final Set<String> databaseNames;

        IsRecord( UUID memberId, int boltPort, RoleInfo role, Set<String> groups, Set<String> databaseNames )
        {
            this.memberId = memberId;
            this.boltPort = boltPort;
            this.role = role;
            this.groups = groups;
            this.databaseNames = databaseNames;
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

            Set<String> recordDatabaseNames = new HashSet<>();
            for ( AnyValue value : (ListValue) record[4] )
            {
                recordDatabaseNames.add( ((TextValue) value).stringValue() );
            }
            if ( !databaseNames.equals( recordDatabaseNames ) )
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
                    ", databaseNames=" + databaseNames +
                    '}' );
        }
    }
}
