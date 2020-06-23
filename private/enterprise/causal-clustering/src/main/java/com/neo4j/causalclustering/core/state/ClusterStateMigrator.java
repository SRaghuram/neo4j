/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.state.version.ClusterStateVersion;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Objects;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.state.SimpleStorage;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.apache.commons.lang3.ArrayUtils.isNotEmpty;

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
        log.info( "Persisted cluster state version is: %s", persistedVersion );

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
            // delete old cluster state files and directories except member ID
            // member ID storage is created outside of the lifecycle and can't be deleted in a lifecycle method
            // it is fine to keep member ID because it is a simple UUID and does not need to be migrated
            var oldClusterStateFiles = fs.listFiles( clusterStateLayout.getClusterStateDirectory(), this::isNotMemberIdStorage );
            if ( isNotEmpty( oldClusterStateFiles ) )
            {
                for ( var oldClusterStateFile : oldClusterStateFiles )
                {
                    fs.deleteRecursively( oldClusterStateFile );
                }
                log.info( "Deleted old cluster state entries %s", Arrays.toString( oldClusterStateFiles ) );
            }

            clusterStateVersionStorage.writeState( CURRENT_VERSION );
            log.info( "Created a version storage for version %s", CURRENT_VERSION );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Unable to migrate the cluster state directory", e );
        }
    }

    private boolean isNotMemberIdStorage( File parentDir, String name )
    {
        // member ID file is located at cluster-state/core-member-id-state/core-member-id
        // thus this filter needs to return false for core-member-id-state directory
        var clusterStateDir = clusterStateLayout.getClusterStateDirectory();
        var memberIdDir = clusterStateLayout.memberIdStateFile().getParentFile().getName();
        return !(parentDir.equals( clusterStateDir ) && name.equals( memberIdDir ));
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
