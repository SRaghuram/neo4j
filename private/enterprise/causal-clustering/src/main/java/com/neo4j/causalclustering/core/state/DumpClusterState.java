/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.state.storage.DurableStateStorage;
import com.neo4j.causalclustering.core.state.storage.SimpleStorage;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;

public class DumpClusterState
{
    private final CoreStateStorageService storageService;
    private final LifeSupport lifeSupport;
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
        this.lifeSupport = new LifeSupport();
        ClusterStateDirectory clusterStateDirectory = new ClusterStateDirectory( fs, dataDirectory ).initialize();
        this.storageService = new CoreStateStorageService( fs, clusterStateDirectory, lifeSupport, NullLogProvider.getInstance(), Config.defaults() );
        this.out = out;
        this.databaseToDump = databaseToDump;
    }

    void dump()
    {
        lifeSupport.start();

        dumpSimpleState( CoreStateFiles.CORE_MEMBER_ID );
        dumpSimpleState( CoreStateFiles.DB_NAME );
        dumpSimpleState( CoreStateFiles.CLUSTER_ID );
        dumpState( CoreStateFiles.LAST_FLUSHED );
        dumpState( CoreStateFiles.LOCK_TOKEN );
        dumpState( CoreStateFiles.ID_ALLOCATION );
        dumpState( CoreStateFiles.SESSION_TRACKER );

        /* raft state */
        dumpState( CoreStateFiles.RAFT_MEMBERSHIP );
        dumpState( CoreStateFiles.RAFT_TERM );
        dumpState( CoreStateFiles.RAFT_VOTE );

        lifeSupport.shutdown();
    }

    private <E> void dumpState( CoreStateFiles<E> fileType ) throws ClusterStateException
    {
        DurableStateStorage<E> storage = storageService.durableStorage( fileType, databaseToDump );

        if ( storage.exists() )
        {
            out.println( String.format( "%s: %s", fileType, storage.getInitialState() ) );
        }
    }

    private <E> void dumpSimpleState( CoreStateFiles<E> fileType ) throws ClusterStateException
    {
        SimpleStorage<E> storage = storageService.simpleStorage( fileType );

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
}
