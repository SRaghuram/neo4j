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

import org.neo4j.kernel.database.DatabaseId;

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

    public File clusterIdStateFile()
    {
        return globalClusterStateFile( CoreStateFiles.CLUSTER_ID );
    }

    public File memberIdStateFile()
    {
        return globalClusterStateFile( CoreStateFiles.CORE_MEMBER_ID );
    }

    public File idAllocationStateDirectory( DatabaseId databaseId )
    {
        return databaseClusterStateDirectory( CoreStateFiles.ID_ALLOCATION, databaseId );
    }

    public File lockTokenStateDirectory( DatabaseId databaseId )
    {
        return databaseClusterStateDirectory( CoreStateFiles.LOCK_TOKEN, databaseId );
    }

    public File lastFlushedStateDirectory( DatabaseId databaseId )
    {
        return databaseClusterStateDirectory( CoreStateFiles.LAST_FLUSHED, databaseId );
    }

    public File raftMembershipStateDirectory( DatabaseId databaseId )
    {
        return databaseClusterStateDirectory( CoreStateFiles.RAFT_MEMBERSHIP, databaseId );
    }

    public File raftLogDirectory( DatabaseId databaseId )
    {
        return databaseClusterStateDirectory( CoreStateFiles.RAFT_LOG, databaseId );
    }

    public File sessionTrackerDirectory( DatabaseId databaseId )
    {
        return databaseClusterStateDirectory( CoreStateFiles.SESSION_TRACKER, databaseId );
    }

    public File raftTermStateDirectory( DatabaseId databaseId )
    {
        return databaseClusterStateDirectory( CoreStateFiles.RAFT_TERM, databaseId );
    }

    public File raftVoteStateDirectory( DatabaseId databaseId )
    {
        return databaseClusterStateDirectory( CoreStateFiles.RAFT_VOTE, databaseId );
    }

    public Set<File> listGlobalAndDatabaseDirectories( DatabaseId databaseId, Predicate<CoreStateFiles<?>> stateFilesFilter )
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
                .map( type -> databaseClusterStateDirectory( type, databaseId ) );

        return Stream.concat( globalDirectories, databaseDirectories ).collect( toSet() );
    }

    private File globalClusterStateFile( CoreStateFiles<?> coreStateFiles )
    {
        checkScope( coreStateFiles, GLOBAL );
        File directory = globalClusterStateDirectory( coreStateFiles );
        return new File( directory, coreStateFiles.name() );
    }

    private File globalClusterStateDirectory( CoreStateFiles<?> coreStateFiles )
    {
        checkScope( coreStateFiles, GLOBAL );
        return new File( clusterStateDirectory, stateDirectoryName( coreStateFiles ) );
    }

    private File databaseClusterStateDirectory( CoreStateFiles<?> coreStateFiles, DatabaseId databaseId )
    {
        checkScope( coreStateFiles, DATABASE );
        File databaseDirectory = new File( dbDirectory(), databaseId.name() );
        return new File( databaseDirectory, stateDirectoryName( coreStateFiles ) );
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

    private static void checkScope( CoreStateFiles<?> coreStateFiles, CoreStateFiles.Scope database )
    {
        checkArgument( coreStateFiles.scope() == database, "Illegal scope: " + coreStateFiles.scope() );
    }
}
