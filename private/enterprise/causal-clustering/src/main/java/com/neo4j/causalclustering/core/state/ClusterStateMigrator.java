/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.state.storage.SimpleStorage;
import com.neo4j.causalclustering.core.state.version.ClusterStateVersion;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class ClusterStateMigrator extends LifecycleAdapter
{
    private static final ClusterStateVersion CURRENT_VERSION = new ClusterStateVersion( 1, 0 );

    private final ClusterStateLayout clusterStateLayout;
    private final SimpleStorage<ClusterStateVersion> clusterStateVersionStorage;
    private final FileSystemAbstraction fs;
    private final Log log;

    public ClusterStateMigrator( FileSystemAbstraction fs, ClusterStateLayout clusterStateLayout,
            SimpleStorage<ClusterStateVersion> clusterStateVersionStorage, LogProvider logProvider )
    {
        this.clusterStateLayout = clusterStateLayout;
        this.clusterStateVersionStorage = clusterStateVersionStorage;
        this.fs = fs;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void init()
    {
        var persistedVersion = readClusterStateVersion();
        log.info( "Persisted cluster state version is: " + persistedVersion );

        if ( persistedVersion == null )
        {
            migrateWhenClusterStateVersionIsAbsent();
        }
        else
        {
            validatePersistedClusterStateVersion( persistedVersion );
        }
    }

    private void migrateWhenClusterStateVersionIsAbsent()
    {
        try
        {
            fs.deleteRecursively( clusterStateLayout.getClusterStateDirectory() );
            clusterStateVersionStorage.writeState( CURRENT_VERSION );
            log.info( "Deleted existing cluster state and created a version storage for version " + CURRENT_VERSION );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Unable to migrate the cluster state directory", e );
        }
    }

    private ClusterStateVersion readClusterStateVersion()
    {
        if ( clusterStateVersionStorage.exists() )
        {
            try
            {
                return clusterStateVersionStorage.readState();
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( "Unable to read cluster state version", e );
            }
        }
        return null;
    }

    private static void validatePersistedClusterStateVersion( ClusterStateVersion persistedVersion )
    {
        if ( !Objects.equals( persistedVersion, CURRENT_VERSION ) )
        {
            throw new IllegalStateException( "Illegal cluster state version: " + persistedVersion + ". Migration for this version does not exist" );
        }
    }
}
