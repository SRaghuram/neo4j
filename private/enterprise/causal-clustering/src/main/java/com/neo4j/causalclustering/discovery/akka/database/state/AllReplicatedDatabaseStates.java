/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.database.state;

import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.neo4j.kernel.database.DatabaseId;

import static java.util.function.Function.identity;

public class AllReplicatedDatabaseStates
{
    private final Map<DatabaseId,ReplicatedDatabaseState> databaseStates;

    AllReplicatedDatabaseStates( Collection<ReplicatedDatabaseState> databaseStates )
    {
        this.databaseStates = databaseStates.stream().collect( Collectors.toMap( ReplicatedDatabaseState::databaseId, identity() ) );
    }

    public Map<DatabaseId,ReplicatedDatabaseState> databaseStates()
    {
        return databaseStates;
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
        AllReplicatedDatabaseStates that = (AllReplicatedDatabaseStates) o;
        return Objects.equals( databaseStates, that.databaseStates );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseStates );
    }
}
