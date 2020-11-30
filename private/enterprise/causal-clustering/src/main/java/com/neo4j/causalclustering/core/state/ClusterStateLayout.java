/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.neo4j.causalclustering.core.state.CoreStateFiles.Scope.DATABASE;
import static com.neo4j.causalclustering.core.state.CoreStateFiles.Scope.GLOBAL;
import static java.util.stream.Collectors.toList;
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
 *
 * Where $NEO4J_HOME/data/cluster-state is configurable with 'causal_clustering.cluster_state_directory'
 * </pre>
 */
public class ClusterStateLayout
{
    private static final String DB_DIRECTORY_NAME = "db";
    private static final String STATE_DIRECTORY_SUFFIX = "-state";

    private final Path clusterStateDirectory;

    private ClusterStateLayout( Path clusterStateDirectory )
    {
        this.clusterStateDirectory = clusterStateDirectory;
    }

    public static ClusterStateLayout of( Path clusterStateDirectory )
    {
        return new ClusterStateLayout( clusterStateDirectory );
    }

    public Path getClusterStateDirectory()
    {
        return clusterStateDirectory;
    }

    public Path clusterStateVersionFile()
    {
        return globalClusterStateFile( CoreStateFiles.VERSION );
    }

    public Path raftGroupIdFile( String databaseName )
    {
        return databaseClusterStateFile( CoreStateFiles.RAFT_GROUP_ID, databaseName );
    }

    public Path quarantineMarkerStateFile( String databaseName )
    {
        return databaseClusterStateFile( CoreStateFiles.QUARANTINE_MARKER, databaseName );
    }

    public Path oldMemberIdStateFile()
    {
        return globalClusterStateFile( CoreStateFiles.OLD_CORE_MEMBER_ID );
    }

    public Path raftMemberIdStateFile( String databaseName )
    {
        return databaseClusterStateFile( CoreStateFiles.RAFT_MEMBER_ID, databaseName );
    }

    public Path leaseStateDirectory( String databaseName )
    {
        return databaseClusterStateDirectory( CoreStateFiles.LEASE, databaseName );
    }

    public Path lastFlushedStateDirectory( String databaseName )
    {
        return databaseClusterStateDirectory( CoreStateFiles.LAST_FLUSHED, databaseName );
    }

    public Path raftMembershipStateDirectory( String databaseName )
    {
        return databaseClusterStateDirectory( CoreStateFiles.RAFT_MEMBERSHIP, databaseName );
    }

    public Path raftLogDirectory( String databaseName )
    {
        return databaseClusterStateDirectory( CoreStateFiles.RAFT_LOG, databaseName );
    }

    public Path sessionTrackerDirectory( String databaseName )
    {
        return databaseClusterStateDirectory( CoreStateFiles.SESSION_TRACKER, databaseName );
    }

    public Path raftTermStateDirectory( String databaseName )
    {
        return databaseClusterStateDirectory( CoreStateFiles.RAFT_TERM, databaseName );
    }

    public Path raftVoteStateDirectory( String databaseName )
    {
        return databaseClusterStateDirectory( CoreStateFiles.RAFT_VOTE, databaseName );
    }

    public Set<Path> listGlobalAndDatabaseDirectories( String databaseName, Predicate<CoreStateFiles<?>> stateFilesFilter )
    {
        Stream<Path> globalDirectories = CoreStateFiles.values()
                .stream()
                .filter( type -> type.scope() == GLOBAL )
                .filter( stateFilesFilter )
                .map( this::globalClusterStateDirectory );

        Stream<Path> databaseDirectories = CoreStateFiles.values()
                .stream()
                .filter( type -> type.scope() == DATABASE )
                .filter( stateFilesFilter )
                .map( type -> databaseClusterStateDirectory( type, databaseName ) );

        return Stream.concat( globalDirectories, databaseDirectories ).collect( toSet() );
    }

    private Path globalClusterStateFile( CoreStateFiles<?> coreStateFiles )
    {
        checkScope( coreStateFiles, GLOBAL );
        Path directory = globalClusterStateDirectory( coreStateFiles );
        return directory.resolve( coreStateFiles.name() );
    }

    private Path databaseClusterStateFile( CoreStateFiles<?> coreStateFiles, String databaseName )
    {
        checkScope( coreStateFiles, DATABASE );
        Path directory = databaseClusterStateDirectory( coreStateFiles, databaseName );
        return directory.resolve( coreStateFiles.name() );
    }

    private Path globalClusterStateDirectory( CoreStateFiles<?> coreStateFiles )
    {
        checkScope( coreStateFiles, GLOBAL );
        return clusterStateDirectory.resolve( stateDirectoryName( coreStateFiles ) );
    }

    private Path databaseClusterStateDirectory( CoreStateFiles<?> coreStateFiles, String databaseName )
    {
        checkScope( coreStateFiles, DATABASE );
        checkDirectoryName( databaseName );
        Path databaseDirectory = dbDirectory().resolve( databaseName );
        return databaseDirectory.resolve( stateDirectoryName( coreStateFiles ) );
    }

    public Path raftGroupDir( String databaseName )
    {
        checkDirectoryName( databaseName );
        return dbDirectory().resolve( databaseName );
    }

    private Path dbDirectory()
    {
        return clusterStateDirectory.resolve( DB_DIRECTORY_NAME );
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

    private static void checkDirectoryName( String dir )
    {
        if ( dir.matches( ".*[\\\\/]+.*" ) )
        {
            throw new IllegalArgumentException( "Illegal directory name" );
        }
    }

    public List<String> allRaftGroups()
    {
        try ( Stream<Path> list = Files.list( dbDirectory() ) )
        {
            return list.filter( Files::isDirectory ).map( c -> c.getFileName().toString() ).collect( toList() );
        }
        catch ( NoSuchFileException e )
        {
            return List.of();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
