/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.member;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
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

import static com.neo4j.dbms.EnterpriseOperatorState.INITIAL;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STORE_COPYING;

public final class DefaultServerSnapshot implements ServerSnapshot
{
    static final Set<OperatorState> DISCOVERABLE_DATABASE_STATES = Set.of( INITIAL, STARTED, STORE_COPYING );

    private final Map<DatabaseId,DatabaseState> databaseStates;
    private final Map<DatabaseId,RaftMemberId> databaseMemberships;
    private final Map<DatabaseId,LeaderInfo> databaseLeaderships;

    private DefaultServerSnapshot( Map<DatabaseId,RaftMemberId> databaseMemberships, Map<DatabaseId,DatabaseState> databaseStates,
            Map<DatabaseId,LeaderInfo> databaseLeaderships )
    {
        this.databaseStates = databaseStates;
        this.databaseMemberships = databaseMemberships;
        this.databaseLeaderships = databaseLeaderships;
    }

    public static ServerSnapshotFactory factoryFor( CoreServerIdentity identityModule )
    {
        return ( databaseStateService, databaseLeaderships ) -> new DefaultServerSnapshot( databaseMemberships( databaseStateService, identityModule ),
                                           databaseStates( databaseStateService ), Map.copyOf( databaseLeaderships ) );
    }

    public static DefaultServerSnapshot rrSnapshot( DatabaseStateService databaseStateService, Map<DatabaseId,LeaderInfo> databaseLeaderships )
    {
        return new DefaultServerSnapshot( Map.of(), databaseStates( databaseStateService ), Map.copyOf( databaseLeaderships ) );
    }

    private static Map<DatabaseId,DatabaseState> databaseStates( DatabaseStateService databaseStateService )
    {
        return databaseStateService.stateOfAllDatabases().entrySet().stream()
                                   .collect( Collectors.toUnmodifiableMap( entry -> entry.getKey().databaseId(), Map.Entry::getValue ) );
    }

    private static Map<DatabaseId,RaftMemberId> databaseMemberships( DatabaseStateService databaseStateService, CoreServerIdentity identityModule )
    {
        return databaseStateService.stateOfAllDatabases().keySet().stream().collect(
                Collectors.toUnmodifiableMap( NamedDatabaseId::databaseId, dbId -> identityModule.raftMemberId( dbId.databaseId() ) ) );
    }

    @Override
    public Map<DatabaseId,LeaderInfo> databaseLeaderships()
    {
        return databaseLeaderships;
    }

    @Override
    public Set<DatabaseId> discoverableDatabases()
    {
        return databaseStates.entrySet().stream()
                             .filter( entry ->  DISCOVERABLE_DATABASE_STATES.contains( entry.getValue().operatorState() ) )
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
        DefaultServerSnapshot that = (DefaultServerSnapshot) o;
        return Objects.equals( databaseStates, that.databaseStates ) &&
               Objects.equals( databaseMemberships, that.databaseMemberships ) &&
               Objects.equals( databaseLeaderships, that.databaseLeaderships );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseStates, databaseMemberships, databaseLeaderships );
    }

    @Override
    public String toString()
    {
        return "DefaultDiscoveryMember{" +
               "databaseStates=" + databaseStates +
               ", databaseMemberships=" + databaseMemberships +
               ", databaseLeaderships=" + databaseLeaderships +
               '}';
    }
}
