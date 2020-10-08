/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.member;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.identity.ClusteringIdentityModule;
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

public final class DefaultDiscoveryMember implements DiscoveryMember
{
    private final Map<DatabaseId,DatabaseState> databaseStates;
    private final Map<DatabaseId,RaftMemberId> databaseMemberships;
    private final Map<DatabaseId,LeaderInfo> databaseLeaderships;

    private DefaultDiscoveryMember( Map<DatabaseId,RaftMemberId> databaseMemberships, Map<DatabaseId,DatabaseState> databaseStates,
            Map<DatabaseId,LeaderInfo> databaseLeaderships )
    {
        this.databaseStates = databaseStates;
        this.databaseMemberships = databaseMemberships;
        this.databaseLeaderships = databaseLeaderships;
    }

    public static DefaultDiscoveryMember factory( ClusteringIdentityModule identityModule,
            DatabaseStateService databaseStateService, Map<DatabaseId,LeaderInfo> databaseLeaderships )
    {
        return new DefaultDiscoveryMember( databaseMemberships( databaseStateService, identityModule ),
                                           databaseStates( databaseStateService ), Map.copyOf( databaseLeaderships ) );
    }

    private static Map<DatabaseId,DatabaseState> databaseStates( DatabaseStateService databaseStateService )
    {
        return databaseStateService.stateOfAllDatabases().entrySet().stream()
                                   .collect( Collectors.toUnmodifiableMap( entry -> entry.getKey().databaseId(), Map.Entry::getValue ) );
    }

    private static Map<DatabaseId,RaftMemberId> databaseMemberships( DatabaseStateService databaseStateService, ClusteringIdentityModule identityModule )
    {
        return databaseStateService.stateOfAllDatabases().keySet().stream()
                                   .collect( Collectors.toUnmodifiableMap( NamedDatabaseId::databaseId, identityModule::memberId ) );
    }

    @Override
    public Map<DatabaseId,LeaderInfo> databaseLeaderships()
    {
        return databaseLeaderships;
    }

    @Override
    public Set<DatabaseId> databasesInState( OperatorState operatorState )
    {
        return databaseStates.entrySet().stream()
                             .filter( entry -> Objects.equals( entry.getValue().operatorState(), operatorState ) )
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
        DefaultDiscoveryMember that = (DefaultDiscoveryMember) o;
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
