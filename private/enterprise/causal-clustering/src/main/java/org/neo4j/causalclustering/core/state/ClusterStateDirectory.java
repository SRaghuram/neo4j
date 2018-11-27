/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;

import static java.lang.String.format;

/**
 * This represents the base directory for cluster state and contains
 * functionality capturing the migration paths.
 *
 * <pre>
 * Typical setup
 *
 *   root state        $NEO4J_HOME/data/cluster-state
 *   database state    $NEO4J_HOME/data/cluster-state/db/graph.db
 * </pre>
 */
public class ClusterStateDirectory
{
    static final String CLUSTER_STATE_DIRECTORY_NAME = "cluster-state";
    private final File rootStateDir;

    private final FileSystemAbstraction fileSystem;
    private final File storeDir;
    private final boolean readOnly;

    private boolean initialized;

    public ClusterStateDirectory( FileSystemAbstraction fileSystem, File dataDir )
    {
        this( fileSystem, dataDir, null, true );
    }

    public ClusterStateDirectory( FileSystemAbstraction fileSystem, File dataDir, boolean readOnly )
    {
        this( fileSystem, dataDir, dataDir, readOnly );
    }

    public ClusterStateDirectory( FileSystemAbstraction fileSystem, File dataDir, File storeDir, boolean readOnly )
    {
        this.fileSystem = fileSystem;
        this.storeDir = storeDir;
        this.readOnly = readOnly;
        this.rootStateDir = new File( dataDir, CLUSTER_STATE_DIRECTORY_NAME );
    }

    /**
     * Returns true if the cluster state base directory exists or
     * could be created. This method also takes care of any necessary
     * migration.
     * <p>
     * It is a requirement to initialize before using the class, unless
     * the non-migrating version is used.
     */
    public ClusterStateDirectory initialize()
    {
        assert !initialized;
        if ( !readOnly )
        {
            migrateRootStateIfNeeded();
        }
        ensureDirectoryExists( rootStateDir );
        initialized = true;
        return this;
    }

    /**
     * For use by special tooling which does not need the functionality
     * of migration or ensuring the directory for cluster state actually
     * exists.
     */
    public static ClusterStateDirectory withoutInitializing( FileSystemAbstraction fileSystem, File dataDir )
    {
        ClusterStateDirectory clusterStateDirectory = new ClusterStateDirectory( fileSystem, dataDir );
        clusterStateDirectory.initialized = true;
        return clusterStateDirectory;
    }

    /**
     * The cluster state directory was previously badly placed under the
     * store directory, and this method takes care of the migration path from
     * that. It will now reside under the data directory.
     */
    private void migrateRootStateIfNeeded()
    {
        File oldStateDir = new File( storeDir, CLUSTER_STATE_DIRECTORY_NAME );
        if ( !fileSystem.fileExists( oldStateDir ) || oldStateDir.equals( rootStateDir ) )
        {
            return;
        }

        if ( fileSystem.fileExists( rootStateDir ) )
        {
            throw new ClusterStateException( "Cluster state exists in both old and new locations" );
        }

        try
        {
            fileSystem.moveToDirectory( oldStateDir, rootStateDir.getParentFile() );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Failed to migrate cluster state directory", e );
        }
    }

    private void ensureDirectoryExists( File dir )
    {
        if ( fileSystem.fileExists( dir ) )
        {
            return;
        }
        else if ( readOnly )
        {
            throw new ClusterStateException( format( "The directory %s does not exist and cannot be created in read-only mode.", dir.getAbsolutePath() ) );
        }

        try
        {
            fileSystem.mkdirs( dir );
        }
        catch ( IOException e )
        {
            throw new ClusterStateException( e );
        }
    }

    File databaseStateDirectory( String databaseName )
    {
        File dbStateDir = new File( new File( rootStateDir, "db" ), databaseName );
        ensureDirectoryExists( dbStateDir );
        return dbStateDir;
    }

    public File get()
    {
        if ( !initialized )
        {
            throw new IllegalStateException( "Cluster state has not been initialized" );
        }
        return rootStateDir;
    }
}
