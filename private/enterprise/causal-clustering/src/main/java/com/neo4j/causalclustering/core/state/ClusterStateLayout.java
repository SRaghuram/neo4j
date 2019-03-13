/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import java.io.File;
import java.util.Set;

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

    public File clusterIdStateFile()
    {
        return globalClusterStateElement( CoreStateFiles.CLUSTER_ID );
    }

    public File memberIdStateFile()
    {
        return globalClusterStateElement( CoreStateFiles.CORE_MEMBER_ID );
    }

    public File multiClusteringDbNameStateFile()
    {
        return globalClusterStateElement( CoreStateFiles.DB_NAME );
    }

    public File idAllocationStateDirectory( String databaseName )
    {
        return databaseClusterStateElement( CoreStateFiles.ID_ALLOCATION, databaseName );
    }

    public File lockTokenStateDirectory( String databaseName )
    {
        return databaseClusterStateElement( CoreStateFiles.LOCK_TOKEN, databaseName );
    }

    public File lastFlushedStateDirectory( String databaseName )
    {
        return databaseClusterStateElement( CoreStateFiles.LAST_FLUSHED, databaseName );
    }

    public File raftMembershipStateDirectory( String databaseName )
    {
        return databaseClusterStateElement( CoreStateFiles.RAFT_MEMBERSHIP, databaseName );
    }

    public File raftLogDirectory( String databaseName )
    {
        return databaseClusterStateElement( CoreStateFiles.RAFT_LOG, databaseName );
    }

    public File sessionTrackerDirectory( String databaseName )
    {
        return databaseClusterStateElement( CoreStateFiles.SESSION_TRACKER, databaseName );
    }

    public File raftTermStateDirectory( String databaseName )
    {
        return databaseClusterStateElement( CoreStateFiles.RAFT_TERM, databaseName );
    }

    public File raftVoteStateDirectory( String databaseName )
    {
        return databaseClusterStateElement( CoreStateFiles.RAFT_VOTE, databaseName );
    }

    public Set<File> allGlobalStateEntries()
    {
        return CoreStateFiles.values()
                .stream()
                .filter( type -> type.scope() == GLOBAL )
                .map( this::globalClusterStateElement )
                .collect( toSet() );
    }

    public Set<File> allDatabaseStateEntries( String databaseName )
    {
        return CoreStateFiles.values()
                .stream()
                .filter( type -> type.scope() == DATABASE )
                .map( type -> databaseClusterStateElement( type, databaseName ) )
                .collect( toSet() );
    }

    private File globalClusterStateElement( CoreStateFiles<?> coreStateFiles )
    {
        checkScope( coreStateFiles, GLOBAL );
        File directory = new File( clusterStateDirectory, stateDirectoryName( coreStateFiles ) );
        return new File( directory, coreStateFiles.name() );
    }

    private File databaseClusterStateElement( CoreStateFiles<?> coreStateFiles, String databaseName )
    {
        checkScope( coreStateFiles, DATABASE );
        File databaseDirectory = new File( dbDirectory(), databaseName );
        return new File( databaseDirectory, stateDirectoryName( coreStateFiles ) );
    }

    private File dbDirectory()
    {
        return new File( clusterStateDirectory, DB_DIRECTORY_NAME );
    }

    private static String stateDirectoryName( CoreStateFiles<?> coreStateFiles )
    {
        return coreStateFiles.name() + STATE_DIRECTORY_SUFFIX;
    }

    private static void checkScope( CoreStateFiles<?> coreStateFiles, CoreStateFiles.Scope database )
    {
        checkArgument( coreStateFiles.scope() == database, "Illegal scope: " + coreStateFiles.scope() );
    }
}
