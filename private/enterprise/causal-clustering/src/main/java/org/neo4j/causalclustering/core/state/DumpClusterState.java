/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Optional;

import org.neo4j.causalclustering.core.state.storage.DurableStateStorage;
import org.neo4j.causalclustering.core.state.storage.SimpleStorage;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.causalclustering.core.state.CoreStateFiles.CLUSTER_ID;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.CORE_MEMBER_ID;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.DB_NAME;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.ID_ALLOCATION;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.LAST_FLUSHED;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.LOCK_TOKEN;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.RAFT_MEMBERSHIP;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.SESSION_TRACKER;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.RAFT_TERM;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.RAFT_VOTE;

public class DumpClusterState
{
    private final CoreStateStorageService storageService;
    private final LifeSupport lifeSupport;
    private final PrintStream out;
    private final String databaseToDump;

    /**
     * @param args [0] = data directory
     * @throws IOException When IO exception occurs.
     */
    public static void main( String[] args ) throws IOException
    {

        File dataDirectory;
        Optional<String> databaseToDumpOpt;
        Optional<String> defaultDatabaseNameOpt;
        if ( args.length == 1 )
        {
            dataDirectory = new File( args[0] );
            databaseToDumpOpt = Optional.empty();
            defaultDatabaseNameOpt = Optional.empty();
        }
        else if ( args.length == 2 )
        {
            dataDirectory = new File( args[0] );
            databaseToDumpOpt = Optional.ofNullable( args[1] );
            defaultDatabaseNameOpt = Optional.empty();
        }
        else if ( args.length == 3 )
        {
            dataDirectory = new File( args[0] );
            databaseToDumpOpt = Optional.ofNullable( args[1] );
            defaultDatabaseNameOpt = Optional.ofNullable( args[2] );
        }
        else
        {
            System.out.println( "usage: DumpClusterState <data directory> ?<database to dump> ?<default database name>" );
            System.exit( 1 );
            return;
        }

        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            String defaultDatabaseName = defaultDatabaseNameOpt.orElse( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
            String databaseToDump = databaseToDumpOpt.orElse( defaultDatabaseName );
            DumpClusterState dumpTool = new DumpClusterState( fileSystem, dataDirectory, System.out, databaseToDump, defaultDatabaseName );
            dumpTool.dump();
        }
        catch ( Exception e )
        {
            System.out.println( "[ERROR] We were unable to properly dump cluster state." );
            System.out.println( "[ERROR] This usually indicates that the cluster-state folder structure is incomplete or otherwise corrupt." );
        }
    }

    DumpClusterState( FileSystemAbstraction fs, File dataDirectory, PrintStream out, String databaseToDump, String defaultDbName )
    {
        this.lifeSupport = new LifeSupport();
        ClusterStateDirectory clusterStateDirectory = new ClusterStateDirectory( dataDirectory ).initialize( fs, defaultDbName );
        this.storageService = new CoreStateStorageService( fs, clusterStateDirectory, lifeSupport, NullLogProvider.getInstance(), Config.defaults() );
        this.out = out;
        this.databaseToDump = databaseToDump;
    }

    void dump()
    {
        lifeSupport.start();

        dumpSimpleState( CORE_MEMBER_ID );
        dumpSimpleState( DB_NAME );
        dumpSimpleState( CLUSTER_ID );
        dumpState( LAST_FLUSHED );
        dumpState( LOCK_TOKEN );
        dumpState( ID_ALLOCATION );
        dumpState( SESSION_TRACKER );

        /* raft state */
        dumpState( RAFT_MEMBERSHIP );
        dumpState( RAFT_TERM );
        dumpState( RAFT_VOTE );

        lifeSupport.shutdown();
    }

    // TODO: look into the marshal here and perhaps enable V1/V2. HF: I'm not sure why we need V1/V2 here?
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
