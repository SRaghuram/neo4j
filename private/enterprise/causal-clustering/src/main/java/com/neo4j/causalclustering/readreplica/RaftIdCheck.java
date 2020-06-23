/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.identity.RaftId;

import java.util.Objects;

import org.neo4j.io.state.SimpleStorage;
import org.neo4j.kernel.database.NamedDatabaseId;

import static java.lang.String.format;

class RaftIdCheck
{
    private final SimpleStorage<RaftId> raftIdStorage;
    private final NamedDatabaseId namedDatabaseId;

    RaftIdCheck( SimpleStorage<RaftId> raftIdStorage, NamedDatabaseId namedDatabaseId )
    {
        this.raftIdStorage = raftIdStorage;
        this.namedDatabaseId = namedDatabaseId;
    }

    public void perform() throws Exception
    {
        if ( raftIdStorage.exists() )
        {
            // If raft id state exists, read it and verify that it corresponds to the database being started
            var raftId = raftIdStorage.readState();
            if ( !Objects.equals( raftId.uuid(), namedDatabaseId.databaseId().uuid() ) )
            {
                throw new IllegalStateException( format( "Pre-existing cluster state found with an unexpected id %s. The id for this database is %s. " +
                        "This may indicate a previous DROP operation for %s did not complete.",
                        raftId.uuid(), namedDatabaseId.databaseId().uuid(), namedDatabaseId.name() ) );
            }
        }
        else
        {
            // If the raft id state doesn't exist, create it. RaftId must correspond to the database id
            var raftId = RaftId.from( namedDatabaseId.databaseId() );
            raftIdStorage.writeState( raftId );
        }
    }
}
