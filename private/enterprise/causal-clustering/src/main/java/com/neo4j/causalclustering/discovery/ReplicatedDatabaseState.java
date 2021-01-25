/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;

public class ReplicatedDatabaseState
{
    private final DatabaseId databaseId;
    private final Map<ServerId,DiscoveryDatabaseState> memberStates;
    private final boolean coreStates;
    private final String name;

    private ReplicatedDatabaseState( DatabaseId databaseId, Map<ServerId,DiscoveryDatabaseState> memberStates, boolean isCoreStates )
    {
        this.databaseId = databaseId;
        this.memberStates = memberStates;
        this.coreStates = isCoreStates;
        this.name = isCoreStates ? "CoreReplicatedDatabaseState" : "ReadReplicaReplicatedDatabaseState";
    }

    public static ReplicatedDatabaseState ofCores( DatabaseId databaseId, Map<ServerId,DiscoveryDatabaseState> memberStates )
    {
        return new ReplicatedDatabaseState( databaseId, memberStates, true );
    }

    public static ReplicatedDatabaseState ofReadReplicas( DatabaseId databaseId, Map<ServerId,DiscoveryDatabaseState> memberStates )
    {
        return new ReplicatedDatabaseState( databaseId, memberStates, false );
    }

    public DatabaseId databaseId()
    {
        return databaseId;
    }

    public Map<ServerId,DiscoveryDatabaseState> memberStates()
    {
        return memberStates;
    }

    public Optional<DiscoveryDatabaseState> stateFor( ServerId serverId )
    {
        return Optional.ofNullable( memberStates.get( serverId ) );
    }

    public boolean isEmpty()
    {
        return memberStates.isEmpty();
    }

    public boolean containsCoreStates()
    {
        return coreStates;
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
        ReplicatedDatabaseState that = (ReplicatedDatabaseState) o;
        return coreStates == that.coreStates && Objects.equals( databaseId, that.databaseId ) && Objects.equals( memberStates, that.memberStates );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseId, memberStates, coreStates );
    }

    @Override
    public String toString()
    {
        return String.format( "%s{%s}", name, memberStates );
    }
}
