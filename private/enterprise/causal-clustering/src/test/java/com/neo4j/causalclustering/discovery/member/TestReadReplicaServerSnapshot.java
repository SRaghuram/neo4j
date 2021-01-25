/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.member;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.causalclustering.discovery.member.DefaultServerSnapshot.DISCOVERABLE_DATABASE_STATES;

public class TestReadReplicaServerSnapshot implements ServerSnapshot
{
    private final Map<DatabaseId,DatabaseState> databaseStates;
    private final Map<DatabaseId,RaftMemberId> databaseMemberships;
    private final Map<DatabaseId,LeaderInfo> databaseLeaderships;

    public TestReadReplicaServerSnapshot( DatabaseStateService databaseStateService, Map<DatabaseId,LeaderInfo> databaseLeaderships )
    {
        this( databaseStates( databaseStateService.stateOfAllDatabases() ), Map.of(), databaseLeaderships );
    }

    private TestReadReplicaServerSnapshot( Map<DatabaseId,DatabaseState> databaseStates,
            Map<DatabaseId,RaftMemberId> databaseMemberships,
            Map<DatabaseId,LeaderInfo> databaseLeaderships )
    {
        this.databaseStates = databaseStates;
        this.databaseMemberships = databaseMemberships;
        this.databaseLeaderships = databaseLeaderships;
    }

    private static Map<DatabaseId,DatabaseState> databaseStates( Map<NamedDatabaseId,DatabaseState> databaseStates )
    {
        return databaseStates.entrySet().stream()
                             .collect( Collectors.toMap( entry -> entry.getKey().databaseId(), Map.Entry::getValue ) );
    }

    public static TestReadReplicaServerSnapshot factory( DatabaseStateService databaseStateService,
            Map<DatabaseId,LeaderInfo> databaseLeaderships )
    {
        return new TestReadReplicaServerSnapshot( databaseStateService, databaseLeaderships );
    }

    @Override
    public Set<DatabaseId> discoverableDatabases()
    {
        return databaseStates.entrySet().stream()
                             .filter( entry -> DISCOVERABLE_DATABASE_STATES.contains( entry.getValue().operatorState() ) )
                             .map( Map.Entry::getKey )
                             .collect( Collectors.toUnmodifiableSet() );
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
        TestReadReplicaServerSnapshot that = (TestReadReplicaServerSnapshot) o;
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
