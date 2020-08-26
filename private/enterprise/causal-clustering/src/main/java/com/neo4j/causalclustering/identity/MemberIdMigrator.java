/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;

import java.io.IOException;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.state.SimpleFileStorage;
import org.neo4j.io.state.SimpleStorage;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;

public class MemberIdMigrator
{
    public static MemberIdMigrator create( LogProvider logProvider, FileSystemAbstraction fs, Neo4jLayout neo4jLayout, MemoryTracker memoryTracker,
            ClusterStateStorageFactory storageFactory )
    {
        return new MemberIdMigrator( logProvider, fs, neo4jLayout, memoryTracker, storageFactory );
    }

    private final Log log;
    private final Neo4jLayout neo4jLayout;
    private final FileSystemAbstraction fs;
    private final MemoryTracker memoryTracker;
    private final ClusterStateStorageFactory storageFactory;

    private MemberIdMigrator( LogProvider logProvider, FileSystemAbstraction fs, Neo4jLayout neo4jLayout, MemoryTracker memoryTracker,
            ClusterStateStorageFactory storageFactory )
    {
        this.log = logProvider.getLog( getClass() );
        this.fs = fs;
        this.neo4jLayout = neo4jLayout;
        this.memoryTracker = memoryTracker;
        this.storageFactory = storageFactory;
    }

    public void migrate()
    {
        convertOldServerIdFilename();

        var memberIdStorage = storageFactory.createMemberIdStorage();
        if ( memberIdStorage.exists() )
        {
            readAndConvertMemberId( memberIdStorage );
        }
    }

    private void convertOldServerIdFilename()
    {
        var oldFile = neo4jLayout.dataDirectory().resolve( "server-id" ).toFile();
        if ( fs.fileExists( oldFile.toPath() ) )
        {
            var newFile = neo4jLayout.serverIdFile().toFile();
            if ( fs.fileExists( newFile.toPath() ) )
            {
                throw new IllegalStateException( "Two ServerIds present, migration to ServerId not possible" );
            }
            try
            {
                fs.renameFile( oldFile.toPath(), newFile.toPath() );
            }
            catch ( IOException ioe )
            {
                throw new RuntimeException( "Problem renaming old ServerId file to new, migration to ServerId not possible", ioe );
            }
        }
    }

    private void readAndConvertMemberId( SimpleStorage<MemberId> memberIdStorage )
    {
        try
        {
            var memberId = memberIdStorage.readState();
            if ( memberId == null )
            {
                throw new IllegalStateException(
                        "MemberId storage was found on disk, but it could not be read correctly, migration to ServerId not possible" );
            }
            else
            {
                generateIdsFromMemberId( memberId );
                memberIdStorage.removeState();
                log.info( String.format(
                        "Existing MemberId found on disk: %s (%s), it has been removed and ServerId has been created with same value",
                        memberId, memberId.getUuid() ) );
            }
        }
        catch ( IOException ioe )
        {
            throw new RuntimeException( "MemberId storage was found on disk, but it could not be read correctly, migration to ServerId not possible", ioe );
        }
    }

    private void generateIdsFromMemberId( MemberId memberId )
    {
        var serverIdStorage = createServerIdStorage();
        if ( serverIdStorage.exists() )
        {
            readAndCompareServerId( memberId, serverIdStorage );
        }
        else
        {
            writeServerId( memberId, serverIdStorage );
        }
        // TODO: Also copy MemberId to all of the RaftGroups (part of the rolling is that in the beginning all MemberIds will have the same value);
        //  Remove MemberId from cluster-state; Change clusterStateLayout
    }

    private void readAndCompareServerId( MemberId memberId, SimpleStorage<ServerId> serverIdStorage )
    {
        try
        {
            var serverId = serverIdStorage.readState();
            if ( serverId == null )
            {
                throw new IllegalStateException(
                        "ServerId storage was found on disk, but it could not be read correctly, migration to ServerId not possible" );
            }
            if ( !serverId.getUuid().equals( memberId.getUuid() ) )
            {
                throw new IllegalStateException(
                        "Both MemberId and ServerId was found during migration with different values, migration to ServerId not possible. " +
                        "This may indicate the need for an unbind or removing either persisted member id or server id" );
            }
        }
        catch ( IOException ioe )
        {
            throw new RuntimeException(
                    "ServerId storage was found on disk, but it could not be read correctly, migration to ServerId not possible", ioe );
        }
    }

    private void writeServerId( MemberId memberId, SimpleStorage<ServerId> serverStorage )
    {
        try
        {
            serverStorage.writeState( memberId );
        }
        catch ( IOException ioe )
        {
            throw new RuntimeException(
                    "MemberId storage was found on disk, but ServerId could not be written correctly, migration to ServerId not possible", ioe );
        }
    }

    private SimpleStorage<ServerId> createServerIdStorage()
    {
        return new SimpleFileStorage<>( fs, neo4jLayout.serverIdFile(), ServerId.Marshal.INSTANCE, memoryTracker );
    }
}
