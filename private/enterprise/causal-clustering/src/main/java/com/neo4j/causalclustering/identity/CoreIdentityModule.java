/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.dbms.identity.AbstractIdentityModule;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;

public class CoreIdentityModule extends AbstractIdentityModule implements CoreServerIdentity
{
    private final Log log;
    private final Map<DatabaseId,RaftMemberId> raftMemberIds = new ConcurrentHashMap<>();
    private final ClusterStateStorageFactory storageFactory;
    private final ServerId serverId;

    public CoreIdentityModule( LogProvider logProvider, FileSystemAbstraction fs, Neo4jLayout layout, MemoryTracker memoryTracker,
            ClusterStateStorageFactory storageFactory )
    {
        this.log = logProvider.getLog( getClass() );
        this.storageFactory = storageFactory;

        var serverIdStorage = createServerIdStorage( fs, layout.serverIdFile(), memoryTracker );
        this.serverId = readOrGenerate( serverIdStorage, log, ServerId.class, ServerId::new, UUID::randomUUID );
    }

    @Override
    public ServerId serverId()
    {
        return serverId;
    }

    @Override
    public RaftMemberId raftMemberId( DatabaseId databaseId )
    {
        RaftMemberId raftMemberId = raftMemberIds.get( databaseId );
        if ( raftMemberId == null )
        {
            throw new IllegalStateException( "Could not find RaftMemberID for " + databaseId );
        }
        return raftMemberId;
    }

    @Override
    public RaftMemberId raftMemberId( NamedDatabaseId databaseId )
    {
        return raftMemberId( databaseId.databaseId() );
    }

    @Override
    public void createMemberId( NamedDatabaseId databaseId, RaftMemberId raftMemberId )
    {
        var storage = storageFactory.createRaftMemberIdStorage( databaseId.name() );

        try
        {
            storage.writeState( raftMemberId );
            log.info( "Created %s for %s", raftMemberId, databaseId );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        raftMemberIds.put( databaseId.databaseId(), raftMemberId );
    }

    @Override
    public RaftMemberId loadMemberId( NamedDatabaseId databaseId )
    {
        var storage = storageFactory.createRaftMemberIdStorage( databaseId.name() );
        try
        {
            RaftMemberId memberId = storage.readState();
            raftMemberIds.put( databaseId.databaseId(), memberId );
            return memberId;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
