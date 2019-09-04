/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.common.LocalDatabase;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class ClusterStateCleaner extends LifecycleAdapter
{
    private final ClusterStateDirectory clusterStateDirectory;
    private final Collection<? extends LocalDatabase> dbs;
    private final FileSystemAbstraction fs;
    private final Log log;

    ClusterStateCleaner( DatabaseService databaseService, CoreStateStorageService coreStateStorageService, FileSystemAbstraction fs, LogProvider logProvider )
    {
        this.dbs = databaseService.registeredDatabases().values();
        this.clusterStateDirectory = coreStateStorageService.clusterStateDirectory();
        this.fs = fs;
        this.log = logProvider.getLog( getClass() );
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
        boolean anyEmpty = dbs.isEmpty();
        for ( LocalDatabase db : dbs )
        {
            anyEmpty = anyEmpty || db.isEmpty();
        }
        return anyEmpty && !clusterStateDirectory.isEmpty();
    }
}
