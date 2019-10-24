/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import java.io.File;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.neo4j.causalclustering.core.state.CoreStateFiles.Scope.DATABASE;
import static com.neo4j.causalclustering.core.state.CoreStateFiles.Scope.GLOBAL;
import static java.util.stream.Collectors.toSet;
import static org.neo4j.util.Preconditions.checkArgument;

/**
 * Describes the layout of the cluster-state directory used to store cluster-specific information like ID allocations and Raft logs.
 *
 * <pre>
 *  Typical setup:
 *
 *    root state        $NEO4J_HOME/data/cluster-state
 *    database state    $NEO4J_HOME/data/cluster-state/db/neo4j
 * </pre>
 */
public class ClusterStateLayout
{
    private static final String CLUSTER_STATE_DIRECTORY_NAME = "cluster-state";
    private static final String DB_DIRECTORY_NAME = "db";
    private static final String STATE_DIRECTORY_SUFFIX = "-state";

    private final File clusterStateDirectory;

    private ClusterStateLayout( File clusterStateDirectory )
    {
        this.clusterStateDirectory = clusterStateDirectory;
    }

    public static ClusterStateLayout of( File parentDirectory )
    {
        return new ClusterStateLayout( new File( parentDirectory, CLUSTER_STATE_DIRECTORY_NAME ) );
    }

    public File getClusterStateDirectory()
    {
        return clusterStateDirectory;
    }

    public File clusterStateVersionFile()
    {
        return globalClusterStateFile( CoreStateFiles.VERSION );
    }

    public File raftIdStateFile( String databaseName )
    {
        return databaseClusterStateFile( CoreStateFiles.RAFT_ID, databaseName );
    }

    public File memberIdStateFile()
    {
        return globalClusterStateFile( CoreStateFiles.CORE_MEMBER_ID );
    }

    public File leaseStateDirectory( String databaseName )
    {
        return databaseClusterStateDirectory( CoreStateFiles.LEASE, databaseName );
    }

    public File lastFlushedStateDirectory( String databaseName )
    {
        return databaseClusterStateDirectory( CoreStateFiles.LAST_FLUSHED, databaseName );
    }

    public File raftMembershipStateDirectory( String databaseName )
    {
        return databaseClusterStateDirectory( CoreStateFiles.RAFT_MEMBERSHIP, databaseName );
    }

    public File raftLogDirectory( String databaseName )
    {
        return databaseClusterStateDirectory( CoreStateFiles.RAFT_LOG, databaseName );
    }

    public File sessionTrackerDirectory( String databaseName )
    {
        return databaseClusterStateDirectory( CoreStateFiles.SESSION_TRACKER, databaseName );
    }

    public File raftTermStateDirectory( String databaseName )
    {
        return databaseClusterStateDirectory( CoreStateFiles.RAFT_TERM, databaseName );
    }

    public File raftVoteStateDirectory( String databaseName )
    {
        return databaseClusterStateDirectory( CoreStateFiles.RAFT_VOTE, databaseName );
    }

    public Set<File> listGlobalAndDatabaseDirectories( String databaseName, Predicate<CoreStateFiles<?>> stateFilesFilter )
    {
        Stream<File> globalDirectories = CoreStateFiles.values()
                .stream()
                .filter( type -> type.scope() == GLOBAL )
                .filter( stateFilesFilter )
                .map( this::globalClusterStateDirectory );

        Stream<File> databaseDirectories = CoreStateFiles.values()
                .stream()
                .filter( type -> type.scope() == DATABASE )
                .filter( stateFilesFilter )
                .map( type -> databaseClusterStateDirectory( type, databaseName ) );

        return Stream.concat( globalDirectories, databaseDirectories ).collect( toSet() );
    }

    private File globalClusterStateFile( CoreStateFiles<?> coreStateFiles )
    {
        checkScope( coreStateFiles, GLOBAL );
        File directory = globalClusterStateDirectory( coreStateFiles );
        return new File( directory, coreStateFiles.name() );
    }

    private File databaseClusterStateFile( CoreStateFiles<?> coreStateFiles, String databaseName )
    {
        checkScope( coreStateFiles, DATABASE );
        File directory = databaseClusterStateDirectory( coreStateFiles, databaseName );
        return new File( directory, coreStateFiles.name() );
    }

    private File globalClusterStateDirectory( CoreStateFiles<?> coreStateFiles )
    {
        checkScope( coreStateFiles, GLOBAL );
        return new File( clusterStateDirectory, stateDirectoryName( coreStateFiles ) );
    }

    private File databaseClusterStateDirectory( CoreStateFiles<?> coreStateFiles, String databaseName )
    {
        checkScope( coreStateFiles, DATABASE );
        File databaseDirectory = new File( dbDirectory(), databaseName );
        return new File( databaseDirectory, stateDirectoryName( coreStateFiles ) );
    }

    public File raftGroupDir( String databaseName )
    {
        return new File( dbDirectory(), databaseName );
    }

    private File dbDirectory()
    {
        return new File( clusterStateDirectory, DB_DIRECTORY_NAME );
    }

    private static String stateDirectoryName( CoreStateFiles<?> coreStateFiles )
    {
        if ( coreStateFiles == CoreStateFiles.RAFT_LOG )
        {
            // raft log is special and lives in a directory without the "-state" suffix
            return coreStateFiles.name();
        }
        return coreStateFiles.name() + STATE_DIRECTORY_SUFFIX;
    }

    private static void checkScope( CoreStateFiles<?> coreStateFiles, CoreStateFiles.Scope scope )
    {
        checkArgument( coreStateFiles.scope() == scope, "Illegal scope: " + coreStateFiles.scope() );
    }
}
