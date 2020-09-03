/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.member.DiscoveryMember;
import com.neo4j.causalclustering.identity.CoreServerIdentity;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.OperatorState;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;

public class TestCoreDiscoveryMember implements DiscoveryMember
{
    private final Map<DatabaseId,DatabaseState> databaseStates;
    private final Map<DatabaseId,RaftMemberId> databaseMemberships;
    private final Map<DatabaseId,LeaderInfo> databaseLeaderships;

    public TestCoreDiscoveryMember( CoreServerIdentity myIdentity,
            DatabaseStateService databaseStateService,
            Map<DatabaseId,LeaderInfo> databaseLeaderships )
    {
        this( databaseStates( databaseStateService.stateOfAllDatabases() ), databaseMemberships( myIdentity, databaseStateService ), databaseLeaderships );
    }

    private TestCoreDiscoveryMember( Map<DatabaseId,DatabaseState> databaseStates,
            Map<DatabaseId,RaftMemberId> databaseMemberships,
            Map<DatabaseId,LeaderInfo> databaseLeaderships )
    {
        this.databaseStates = databaseStates;
        this.databaseMemberships = databaseMemberships;
        this.databaseLeaderships = databaseLeaderships;
    }

    private static Map<DatabaseId,RaftMemberId> databaseMemberships( CoreServerIdentity myIdentity, DatabaseStateService databaseStates )
    {
        return databaseStates.stateOfAllDatabases().keySet().stream()
                             .collect( Collectors.toMap( NamedDatabaseId::databaseId, dbId -> myIdentity.raftMemberId( dbId.databaseId() ) ) );
    }

    private static Map<DatabaseId,DatabaseState> databaseStates( Map<NamedDatabaseId,DatabaseState> databaseStates )
    {
        return databaseStates.entrySet().stream()
                             .collect( Collectors.toMap( entry -> entry.getKey().databaseId(), Map.Entry::getValue ) );
    }

    public static TestCoreDiscoveryMember factory( CoreServerIdentity myIdentity, DatabaseStateService databaseStateService,
            Map<DatabaseId,LeaderInfo> databaseLeaderships )
    {
        return new TestCoreDiscoveryMember( myIdentity, databaseStateService, databaseLeaderships );
    }

    @Override
    public Set<DatabaseId> databasesInState( OperatorState operatorState )
    {
        return databaseStates.values().stream()
                             .filter( state -> Objects.equals( state.operatorState(), operatorState ) )
                             .map( state -> state.databaseId().databaseId() )
                             .collect( Collectors.toSet() );
    }

    @Override
    public Map<DatabaseId,DatabaseState> databaseStates()
    {
        return databaseStates;
    }

    @Override
    public Map<DatabaseId,RaftMemberId> databaseMemberships()
    {
        return databaseMemberships;
    }

    @Override
    public Map<DatabaseId,LeaderInfo> databaseLeaderships()
    {
        return databaseLeaderships;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        TestCoreDiscoveryMember that = (TestCoreDiscoveryMember) o;
        return Objects.equals( databaseStates, that.databaseStates ) &&
               Objects.equals( databaseMemberships, that.databaseMemberships ) &&
               Objects.equals( databaseLeaderships, that.databaseLeaderships );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseStates, databaseMemberships, databaseLeaderships );
    }
}
