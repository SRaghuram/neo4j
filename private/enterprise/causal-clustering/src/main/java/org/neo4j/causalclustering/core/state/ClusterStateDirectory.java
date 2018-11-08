/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;

import static org.neo4j.causalclustering.core.state.CoreStateFiles.ID_ALLOCATION;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.LOCK_TOKEN;

/**
 * This represents the base directory for cluster state and contains
 * functionality capturing the migration paths.
 *
 * <pre>
 * Typical setup
 *
 *   root state        $NEO4J_HOME/data/cluster-state
 *   database state    $NEO4J_HOME/data/cluster-state/graph.db
 * </pre>
 */
public class ClusterStateDirectory
{
    static final String CLUSTER_STATE_DIRECTORY_NAME = "cluster-state";

    private final File rootStateDir;

    private final File storeDir;
    private final boolean readOnly;

    private boolean initialized;

    public ClusterStateDirectory( File dataDir )
    {
        this( dataDir, null, true );
    }

    public ClusterStateDirectory( File dataDir, boolean readOnly )
    {
        this( dataDir, dataDir, readOnly );
    }

    public ClusterStateDirectory( File dataDir, File storeDir, boolean readOnly )
    {
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
    public ClusterStateDirectory initialize( FileSystemAbstraction fs, String defaultDbName )
    {
        assert !initialized;
        if ( !readOnly )
        {
            migrateRootStateIfNeeded( fs );
        }
        ensureDirectoryExists( fs, rootStateDir );
        migrateDataStateIfNeeded( fs, defaultDbName );
        initialized = true;
        return this;
    }

    /**
     * For use by special tooling which does not need the functionality
     * of migration or ensuring the directory for cluster state actually
     * exists.
     */
    public static ClusterStateDirectory withoutInitializing( File dataDir )
    {
        ClusterStateDirectory clusterStateDirectory = new ClusterStateDirectory( dataDir );
        clusterStateDirectory.initialized = true;
        return clusterStateDirectory;
    }

    /**
     * The cluster state directory was previously badly placed under the
     * store directory, and this method takes care of the migration path from
     * that. It will now reside under the data directory.
     */
    private void migrateRootStateIfNeeded( FileSystemAbstraction fs )
    {
        File oldStateDir = new File( storeDir, CLUSTER_STATE_DIRECTORY_NAME );
        if ( !fs.fileExists( oldStateDir ) || oldStateDir.equals( rootStateDir ) )
        {
            return;
        }

        if ( fs.fileExists( rootStateDir ) )
        {
            throw new ClusterStateException( "Cluster state exists in both old and new locations" );
        }

        try
        {
            fs.moveToDirectory( oldStateDir, rootStateDir.getParentFile() );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Failed to migrate cluster state directory", e );
        }
    }

    private void migrateDataStateIfNeeded( FileSystemAbstraction fs, String defaultDbName )
    {
        File idAllocDir = ID_ALLOCATION.at( rootStateDir );
        File lockTokenDir = LOCK_TOKEN.at( rootStateDir );

        File dbStateDir = new File( rootStateDir, defaultDbName );

        if ( fs.fileExists( idAllocDir ) || fs.fileExists( lockTokenDir ) )
        {
            if ( !( fs.fileExists( idAllocDir ) && fs.fileExists( lockTokenDir ) ) )
            {
                throw new ClusterStateException( String.format( "Invalid cluster state found! Both %s and %s should exist!", ID_ALLOCATION.directoryFullName(),
                        LOCK_TOKEN.directoryFullName() ) );
            }

            if ( fs.fileExists( dbStateDir ) )
            {
                throw new ClusterStateException( "Invalid cluster state found! Overlapping top-level and database state." );
            }
            else
            {
                ensureDirectoryExists( fs, dbStateDir );

                try
                {
                    fs.moveToDirectory( idAllocDir, dbStateDir );
                    fs.moveToDirectory( lockTokenDir, dbStateDir );
                }
                catch ( IOException e )
                {
                    throw new ClusterStateException( e );
                }
            }
        }
    }

    private void ensureDirectoryExists( FileSystemAbstraction fs, File dir )
    {
        if ( !fs.fileExists( dir ) )
        {
            if ( readOnly )
            {
                throw new ClusterStateException( String.format( "The directory %s does not exist!", dir.getAbsolutePath() ) );
            }
            else
            {
                try
                {
                    fs.mkdirs( dir );
                }
                catch ( IOException e )
                {
                    throw new ClusterStateException( e );
                }
            }
        }
    }

    PerDatabaseClusterStateDirectory stateFor( FileSystemAbstraction fs, String databaseName )
    {
        File subDir = new File( this.get(), databaseName );
        ensureDirectoryExists( fs,  subDir );
        return new PerDatabaseClusterStateDirectory( this, databaseName, subDir );
    }

    public File get()
    {
        if ( !initialized )
        {
            throw new IllegalStateException( "Cluster state has not been initialized" );
        }
        return rootStateDir;
    }

    /**
     * This represents the sub-directory of cluster state containing state related to a specific database.
     * Unlike the enclosing {{@link ClusterStateDirectory}} this class contains no migration logic, as this is handled by
     * the login *in* {{@link ClusterStateDirectory#initialize(FileSystemAbstraction, String)}}.
     */
    public static class PerDatabaseClusterStateDirectory
    {
        private final ClusterStateDirectory parent;
        private final String databaseName;
        private final File subDir;

        private PerDatabaseClusterStateDirectory( ClusterStateDirectory parent, String databaseName, File subDir )
        {
            this.parent = parent;
            this.databaseName = databaseName;
            this.subDir = subDir;
        }

        public ClusterStateDirectory parent()
        {
            return parent;
        }

        public String databaseName()
        {
            return databaseName;
        }

        public File get()
        {
            return subDir;
        }
    }
}
