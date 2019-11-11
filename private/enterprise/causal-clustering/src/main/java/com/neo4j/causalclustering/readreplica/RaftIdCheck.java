/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.core.state.storage.SimpleStorage;
import com.neo4j.causalclustering.identity.RaftId;

import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;

import static java.lang.String.format;

class RaftIdCheck
{
    private final SimpleStorage<RaftId> raftIdStorage;
    private final DatabaseId databaseId;

    RaftIdCheck( SimpleStorage<RaftId> raftIdStorage, DatabaseId databaseId )
    {
        this.raftIdStorage = raftIdStorage;
        this.databaseId = databaseId;
    }

    public void perform() throws Exception
    {
        if ( raftIdStorage.exists() )
        {
            // If raft id state exists, read it and verify that it corresponds to the database being started
            var raftId = raftIdStorage.readState();
            if ( !Objects.equals( raftId.uuid(), databaseId.uuid() ) )
            {
                throw new IllegalStateException( format( "Pre-existing cluster state found with an unexpected id %s. The id for this database is %s. " +
                        "This may indicate a previous DROP operation for %s did not complete.", raftId.uuid(), databaseId.uuid(), databaseId.name() ) );
            }
        }
        else
        {
            // If the raft id state doesn't exist, create it. RaftId must correspond to the database id
            var raftId = RaftId.from( databaseId );
            raftIdStorage.writeState( raftId );
        }
    }
}
