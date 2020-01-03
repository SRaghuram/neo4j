/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static com.neo4j.causalclustering.discovery.RoleInfo.FOLLOWER;
import static com.neo4j.causalclustering.discovery.RoleInfo.LEADER;
import static com.neo4j.causalclustering.discovery.RoleInfo.READ_REPLICA;
import static com.neo4j.causalclustering.discovery.RoleInfo.valueOf;
import static com.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static com.neo4j.causalclustering.discovery.TestTopology.addressesForReadReplica;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.outputField;
import static org.neo4j.values.storable.Values.stringValue;

class ClusterOverviewProcedureTest
{
    private static final TestDatabaseIdRepository DATABASE_ID_REPOSITORY = new TestDatabaseIdRepository();

    @Test
    void shouldHaveCorrectSignature()
    {
        CoreTopologyService topologyService = mock( CoreTopologyService.class );
        ClusterOverviewProcedure procedure = new ClusterOverviewProcedure( topologyService, DATABASE_ID_REPOSITORY );

        ProcedureSignature signature = procedure.signature();

        assertEquals( "dbms.cluster.overview", signature.name().toString() );
        assertEquals( List.of(), signature.inputSignature() );
        assertTrue( signature.systemProcedure() );
        assertEquals(
                List.of( outputField( "id", Neo4jTypes.NTString ),
                        outputField( "addresses", Neo4jTypes.NTList( Neo4jTypes.NTString ) ),
                        outputField( "databases", Neo4jTypes.NTMap ),
                        outputField( "groups", Neo4jTypes.NTList( Neo4jTypes.NTString ) ) ),
                signature.outputSignature() );
    }

    @Test
    void shouldProvideOverviewOfCoreServersAndReadReplicas() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        MemberId theLeader = new MemberId( new UUID( 1, 0 ) );
        MemberId follower1 = new MemberId( new UUID( 2, 0 ) );
        MemberId follower2 = new MemberId( new UUID( 3, 0 ) );

        Set<NamedDatabaseId> leaderDatabases = databaseIds( "customers", "orders", "system" );
        Set<NamedDatabaseId> follower1Databases = databaseIds( "system", "orders" );
        Set<NamedDatabaseId> follower2Databases = databaseIds( "system" );
        coreMembers.put( theLeader, addressesForCore( 0, false, toRaw( leaderDatabases ) ) );
        coreMembers.put( follower1, addressesForCore( 1, false, toRaw( follower1Databases ) ) );
        coreMembers.put( follower2, addressesForCore( 2, false, toRaw( follower2Databases ) ) );

        Map<MemberId,ReadReplicaInfo> replicaMembers = new HashMap<>();
        MemberId replica4 = new MemberId( new UUID( 4, 0 ) );
        MemberId replica5 = new MemberId( new UUID( 5, 0 ) );

        Set<NamedDatabaseId> replica1Databases = databaseIds( "system", "orders" );
        Set<NamedDatabaseId> replica2Databases = databaseIds( "system", "customers" );
        replicaMembers.put( replica4, addressesForReadReplica( 4, toRaw( replica1Databases ) ) );
        replicaMembers.put( replica5, addressesForReadReplica( 5, toRaw( replica2Databases ) ) );

        when( topologyService.allCoreServers() ).thenReturn( coreMembers );
        when( topologyService.allReadReplicas() ).thenReturn( replicaMembers );
        for ( NamedDatabaseId namedDatabaseId : leaderDatabases )
        {
            when( topologyService.lookupRole( namedDatabaseId, theLeader ) ).thenReturn( LEADER );
        }
        for ( NamedDatabaseId namedDatabaseId : follower1Databases )
        {
            when( topologyService.lookupRole( namedDatabaseId, follower1 ) ).thenReturn( FOLLOWER );
        }
        for ( NamedDatabaseId namedDatabaseId : follower2Databases )
        {
            when( topologyService.lookupRole( namedDatabaseId, follower2 ) ).thenReturn( FOLLOWER );
        }

        ClusterOverviewProcedure procedure = new ClusterOverviewProcedure( topologyService, DATABASE_ID_REPOSITORY );

        // when
        final RawIterator<AnyValue[],ProcedureException> members = procedure.apply( null, new AnyValue[0], null );

        assertThat( members.next(), new IsRecord( theLeader, 5000, databasesWithRole( leaderDatabases, LEADER ), Set.of( "core", "core0" ) ) );
        assertThat( members.next(), new IsRecord( follower1, 5001, databasesWithRole( follower1Databases, FOLLOWER ), Set.of( "core", "core1" ) ) );
        assertThat( members.next(), new IsRecord( follower2, 5002, databasesWithRole( follower2Databases, FOLLOWER ), Set.of( "core", "core2" ) ) );

        assertThat( members.next(), new IsRecord( replica4, 6004, databasesWithRole( replica1Databases, READ_REPLICA ), Set.of( "replica", "replica4" ) ) );
        assertThat( members.next(), new IsRecord( replica5, 6005, databasesWithRole( replica2Databases, READ_REPLICA ), Set.of( "replica", "replica5" ) ) );

        assertFalse( members.hasNext() );
    }

    private static Set<NamedDatabaseId> databaseIds( String... names )
    {
        return Arrays.stream( names )
                .map( DATABASE_ID_REPOSITORY::getRaw )
                .collect( toSet() );
    }

    private static Set<DatabaseId> toRaw( Set<NamedDatabaseId> namedDatabaseIds )
    {
        return namedDatabaseIds.stream().map( NamedDatabaseId::databaseId ).collect( Collectors.toSet() );
    }

    private static Map<NamedDatabaseId,RoleInfo> databasesWithRole( Set<NamedDatabaseId> namedDatabaseIds, RoleInfo role )
    {
        return namedDatabaseIds.stream()
                .collect( toMap( identity(), ignore -> role ) );
    }

    private static class IsRecord extends TypeSafeMatcher<AnyValue[]>
    {
        private final UUID memberId;
        private final int boltPort;
        private final Map<NamedDatabaseId,RoleInfo> databases;
        private final Set<String> groups;

        private IsRecord( MemberId memberId, int boltPort, Map<NamedDatabaseId,RoleInfo> databases, Set<String> groups )
        {
            this.memberId = memberId.getUuid();
            this.boltPort = boltPort;
            this.databases = databases;
            this.groups = groups;
        }

        @Override
        protected boolean matchesSafely( AnyValue[] record )
        {
            if ( record.length != 4 )
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

            Map<NamedDatabaseId,RoleInfo> recordDatabases = new HashMap<>();
            MapValue mapValue = (MapValue) record[2];
            for ( String key : mapValue.keySet() )
            {
                TextValue value = (TextValue) mapValue.get( key );
                recordDatabases.put( DATABASE_ID_REPOSITORY.getRaw( key ), valueOf( value.stringValue() ) );
            }
            if ( !recordDatabases.equals( databases ) )
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

            return true;
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText(
                    "memberId=" + memberId +
                    ", boltPort=" + boltPort +
                    ", databases=" + databases +
                    ", groups=" + groups +
                    '}' );
        }
    }
}
