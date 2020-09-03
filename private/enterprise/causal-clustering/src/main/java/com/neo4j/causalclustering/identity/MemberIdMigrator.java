/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.UUID;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.state.SimpleFileStorage;
import org.neo4j.io.state.SimpleStorage;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Id;

import static java.lang.String.format;

/**
 * As part of 4.3 a new ServerID was introduced.
 *
 * Previously there was just a single MemberID which was stored as follows
 *
 *     data/cluster-state/core-member-id-state/core-member-id
 *
 * but after migration the setup now looks as follows
 *
 *     data/server_id
 *     data/cluster-state/db/system/core-member-id-state/core-member-id
 *     data/cluster-state/db/neo4j/core-member-id-state/core-member-id
 *     ...
 *
 * As part of migration during an upgrade the old MemberID will be copied to all
 * of the above locations, keeping backwards compatibility as far as identifiers
 * communicated across the network is concerned. This separates the concepts of
 * ServerID and MemberID and will for example allow for the unbinding of individual
 * raft groups.
 */
public class MemberIdMigrator extends LifecycleAdapter
{
    private final Log log;
    private final Neo4jLayout neo4jLayout;
    private final FileSystemAbstraction fs;
    private final MemoryTracker memoryTracker;
    private final ClusterStateStorageFactory storageFactory;
    private ClusterStateLayout clusterStateLayout;

    public MemberIdMigrator( LogProvider logProvider, FileSystemAbstraction fs, Neo4jLayout neo4jLayout, ClusterStateLayout clusterStateLayout,
            ClusterStateStorageFactory storageFactory, MemoryTracker memoryTracker )
    {
        this.log = logProvider.getLog( getClass() );
        this.fs = fs;
        this.neo4jLayout = neo4jLayout;
        this.clusterStateLayout = clusterStateLayout;
        this.memoryTracker = memoryTracker;
        this.storageFactory = storageFactory;
    }

    @Override
    public void init()
    {
        var oldMemberIdStorage = storageFactory.createOldMemberIdStorage();
        if ( oldMemberIdStorage.exists() )
        {
            readAndConvertMemberId( oldMemberIdStorage );
        }
    }

    private void readAndConvertMemberId( SimpleStorage<RaftMemberId> oldMemberIdStorage )
    {
        try
        {
            var oldMemberId = oldMemberIdStorage.readState();
            if ( oldMemberId == null )
            {
                throw new IllegalStateException(
                        "MemberId storage was found on disk, but it could not be read correctly, migration to ServerId not possible" );
            }
            else
            {
                generateServerIdFromOldMemberId( oldMemberId );
                generateNewMemberIdsFromOldMemberId( oldMemberId );

                oldMemberIdStorage.removeState();
                Path oldMemberIdDir = clusterStateLayout.oldMemberIdStateFile().getParent();
                fs.deleteFile( oldMemberIdDir );

                log.info( format( "Existing MemberId was found on disk: %s, it has been removed and ServerId has been created with same value",
                        oldMemberId.uuid() ) );
            }
        }
        catch ( IOException ioe )
        {
            throw new RuntimeException( "MemberId storage was found on disk, but it could not be read correctly, migration to ServerId not possible", ioe );
        }
    }

    private void generateServerIdFromOldMemberId( RaftMemberId oldMemberId )
    {
        var serverIdStorage = createServerIdStorage();
        if ( serverIdStorage.exists() )
        {
            readAndCompareServerId( oldMemberId, serverIdStorage );
        }
        else
        {
            writeServerId( serverIdStorage, oldMemberId.uuid() );
        }
    }

    private void generateNewMemberIdsFromOldMemberId( RaftMemberId oldMemberId ) throws IOException
    {
        Collection<String> databaseNames = clusterStateLayout.allRaftGroups();

        for ( String databaseName : databaseNames )
        {
            SimpleStorage<RaftMemberId> newMemberIdStorage = storageFactory.createRaftMemberIdStorage( databaseName );
            if ( newMemberIdStorage.exists() )
            {
                RaftMemberId newMemberId = newMemberIdStorage.readState();
                if ( !oldMemberId.equals( newMemberId ) )
                {
                    throw new IllegalStateException(
                            format( "Found new MemberID %s during migration which does not equal old MemberID %s at %s", newMemberId.uuid(), oldMemberId.uuid(),
                                    clusterStateLayout.raftMemberIdStateFile( databaseName ) ) );
                }
                // this is considered fine, since it could be a previous migration that was aborted mid-way
            }
            else
            {
                newMemberIdStorage.writeState( oldMemberId );
            }
        }
    }

    private void readAndCompareServerId( Id memberId, SimpleStorage<ServerId> serverIdStorage )
    {
        try
        {
            var serverId = serverIdStorage.readState();
            if ( serverId == null )
            {
                throw new IllegalStateException(
                        "ServerId storage was found on disk, but it could not be read correctly. Migration to ServerId is not possible." );
            }
            if ( !serverId.uuid().equals( memberId.uuid() ) )
            {
                throw new IllegalStateException(
                        "Both old MemberId and ServerId were found during migration with different values. Migration to ServerId is not possible. " +
                        "This may indicate the need for an unbind or removing either old member id or server id." );
            }
        }
        catch ( IOException ioe )
        {
            throw new RuntimeException(
                    "ServerId storage was found on disk, but it could not be read correctly. Migration to ServerId is not possible.", ioe );
        }
    }

    private void writeServerId( SimpleStorage<ServerId> serverStorage, UUID uuid )
    {
        try
        {
            serverStorage.writeState( new ServerId( uuid ) );
        }
        catch ( IOException ioe )
        {
            throw new RuntimeException(
                    "Old MemberId was found on disk, but ServerId could not be written correctly. Migration to ServerId is not possible.", ioe );
        }
    }

    private SimpleStorage<ServerId> createServerIdStorage()
    {
        return new SimpleFileStorage<>( fs, neo4jLayout.serverIdFile(), ServerId.Marshal.INSTANCE, memoryTracker );
    }
}
