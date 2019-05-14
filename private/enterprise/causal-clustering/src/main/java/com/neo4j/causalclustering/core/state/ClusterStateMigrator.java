/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.state.storage.SimpleStorage;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class ClusterStateMigrator
{
    private static final long CURRENT_CLUSTER_STATE_VERSION = 1;

    private final ClusterStateLayout clusterStateLayout;
    private final SimpleStorage<Long> clusterStateVersionStorage;
    private final FileSystemAbstraction fs;
    private final Log log;

    public ClusterStateMigrator( FileSystemAbstraction fs, ClusterStateLayout clusterStateLayout, SimpleStorage<Long> clusterStateVersionStorage,
            LogProvider logProvider )
    {
        this.clusterStateLayout = clusterStateLayout;
        this.clusterStateVersionStorage = clusterStateVersionStorage;
        this.fs = fs;
        this.log = logProvider.getLog( getClass() );
    }

    public void migrateIfNeeded()
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
            clusterStateVersionStorage.writeState( CURRENT_CLUSTER_STATE_VERSION );
            log.info( "Deleted existing cluster state and created a version storage for version " + CURRENT_CLUSTER_STATE_VERSION );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Unable to migrate the cluster state directory", e );
        }
    }

    private Long readClusterStateVersion()
    {
        if ( clusterStateVersionStorage.exists() )
        {
            try
            {
                return clusterStateVersionStorage.readState();
            }
            catch ( IOException e )
            {
                log.warn( "Unable to read cluster state version", e );
            }
        }
        return null;
    }

    private static void validatePersistedClusterStateVersion( Long persistedVersion )
    {
        if ( persistedVersion == null || persistedVersion != CURRENT_CLUSTER_STATE_VERSION )
        {
            throw new IllegalStateException( "Illegal cluster state version: " + persistedVersion + ". Migration for this version does not exist" );
        }
    }
}
