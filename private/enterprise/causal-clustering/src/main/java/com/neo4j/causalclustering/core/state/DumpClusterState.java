/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.core.state.storage.SimpleStorage;
import com.neo4j.causalclustering.core.state.storage.StateStorage;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;

import static org.neo4j.logging.internal.DatabaseLogProvider.nullDatabaseLogProvider;

public class DumpClusterState
{
    private final ClusterStateStorageFactory storageFactory;
    private final PrintStream out;
    private final String databaseToDump;

    /**
     * @param args [0] = data directory
     */
    public static void main( String[] args )
    {
        File dataDirectory;
        Optional<String> databaseToDumpOpt;
        Optional<String> databaseNameOpt;
        if ( args.length == 1 )
        {
            dataDirectory = new File( args[0] );
            databaseToDumpOpt = Optional.empty();
            databaseNameOpt = Optional.empty();
        }
        else if ( args.length == 2 )
        {
            dataDirectory = new File( args[0] );
            databaseToDumpOpt = Optional.ofNullable( args[1] );
            databaseNameOpt = Optional.empty();
        }
        else if ( args.length == 3 )
        {
            dataDirectory = new File( args[0] );
            databaseToDumpOpt = Optional.ofNullable( args[1] );
            databaseNameOpt = Optional.ofNullable( args[2] );
        }
        else
        {
            System.out.println( "usage: DumpClusterState <data directory> ?<database to dump> ?<default database name>" );
            System.exit( 1 );
            return;
        }

        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            String databaseName = databaseNameOpt.orElse( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
            String databaseToDump = databaseToDumpOpt.orElse( databaseName );
            DumpClusterState dumpTool = new DumpClusterState( fileSystem, dataDirectory, System.out, databaseToDump );
            dumpTool.dump();
        }
        catch ( Exception e )
        {
            System.out.println( "[ERROR] We were unable to properly dump cluster state." );
            System.out.println( "[ERROR] This usually indicates that the cluster-state folder structure is incomplete or otherwise corrupt." );
        }
    }

    DumpClusterState( FileSystemAbstraction fs, File dataDirectory, PrintStream out, String databaseToDump )
    {
        this.storageFactory = newCoreStateStorageService( fs, dataDirectory );
        this.out = out;
        this.databaseToDump = databaseToDump;
    }

    void dump()
    {
        LifeSupport life = new LifeSupport();
        life.start();
        try
        {
            dumpSimpleState( CoreStateFiles.CORE_MEMBER_ID, storageFactory.createMemberIdStorage() );
            dumpSimpleState( CoreStateFiles.RAFT_ID, storageFactory.createRaftIdStorage( databaseToDump, nullDatabaseLogProvider() ) );

            dumpState( CoreStateFiles.LAST_FLUSHED, storageFactory.createLastFlushedStorage( databaseToDump, life, nullDatabaseLogProvider() ) );
            dumpState( CoreStateFiles.LEASE, storageFactory.createLeaseStorage( databaseToDump, life, nullDatabaseLogProvider() ) );
            dumpState( CoreStateFiles.SESSION_TRACKER, storageFactory.createSessionTrackerStorage( databaseToDump, life, nullDatabaseLogProvider() ) );

            /* raft state */
            dumpState( CoreStateFiles.RAFT_MEMBERSHIP, storageFactory.createRaftMembershipStorage( databaseToDump, life, nullDatabaseLogProvider() ) );
            dumpState( CoreStateFiles.RAFT_TERM, storageFactory.createRaftTermStorage( databaseToDump, life, nullDatabaseLogProvider() ) );
            dumpState( CoreStateFiles.RAFT_VOTE, storageFactory.createRaftVoteStorage( databaseToDump, life, nullDatabaseLogProvider() ) );
        }
        finally
        {
            life.shutdown();
        }
    }

    private <E> void dumpState( CoreStateFiles<E> fileType, StateStorage<E> storage )
    {
        if ( storage.exists() )
        {
            out.println( String.format( "%s: %s", fileType, storage.getInitialState() ) );
        }
    }

    private <E> void dumpSimpleState( CoreStateFiles<E> fileType, SimpleStorage<E> storage )
    {
        if ( storage.exists() )
        {
            String stateStr;
            try
            {
                stateStr = storage.readState().toString();
            }
            catch ( IOException e )
            {
                stateStr = String.format( "Error, state unreadable. %s", e.getMessage() );
            }
            out.println( String.format( "%s: %s", fileType, stateStr ) );
        }
    }

    private static ClusterStateStorageFactory newCoreStateStorageService( FileSystemAbstraction fs, File dataDirectory )
    {
        ClusterStateLayout layout = ClusterStateLayout.of( dataDirectory );
        return new ClusterStateStorageFactory( fs, layout, NullLogProvider.getInstance(), Config.defaults(), EmptyMemoryTracker.INSTANCE );
    }
}
