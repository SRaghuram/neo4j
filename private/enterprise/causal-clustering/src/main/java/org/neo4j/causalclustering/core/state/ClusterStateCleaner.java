/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import java.io.IOException;

import org.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.StoreLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;

public class ClusterStateCleaner extends LifecycleAdapter
{
    private final StoreFiles storeFiles;
    private final StoreLayout storeLayout;
    private final ClusterStateDirectory clusterStateDirectory;
    private final FileSystemAbstraction fs;
    private final Log log;
    private final Config config;

    ClusterStateCleaner( StoreFiles storeFiles, StoreLayout storeLayout, CoreStateStorageService coreStateStorageService, FileSystemAbstraction fs,
            LogProvider logProvider, Config config )
    {
        this.storeFiles = storeFiles;
        this.storeLayout = storeLayout;
        this.clusterStateDirectory = coreStateStorageService.clusterStateDirectory();
        this.fs = fs;
        this.log = logProvider.getLog( getClass() );
        this.config = config;
    }

    @Override
    public void init() throws Throwable
    {
        if ( stateUnclean() )
        {
            log.warn("Found cluster state without corresponding database. " +
                    "This likely indicates a previously failed store copy. " +
                    "Cluster state will be cleared and reinitialized.");
            clusterStateDirectory.clear( fs );
        }
    }

    public boolean stateUnclean() throws IOException
    {
        boolean anyEmpty = false;
        if ( SecuritySettings.isSystemDatabaseEnabled( config ) )
        {
            anyEmpty = dbIsEmpty( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
        }
        anyEmpty = anyEmpty || dbIsEmpty( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        return anyEmpty && !clusterStateDirectory.isEmpty();
    }

    private boolean dbIsEmpty( String databaseName ) throws IOException
    {
        DatabaseLayout dbLayout = storeLayout.databaseLayout( databaseName );
        return storeFiles.isEmpty( dbLayout.databaseDirectory(), dbLayout.storeFiles() );
    }
}
