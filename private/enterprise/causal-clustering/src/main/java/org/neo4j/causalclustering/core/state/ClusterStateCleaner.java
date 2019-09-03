/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import java.io.IOException;

import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Cluster state existing without a corresponding database is an illegal state. It likely indicates a previously failed store copy.
 * In this event we remove and re-initialize the cluster state (equivalent to manually executing an unbind command).
 */
public class ClusterStateCleaner extends LifecycleAdapter
{

    private final LocalDatabase localDatabase;
    private final ClusterStateDirectory clusterStateDirectory;
    private final FileSystemAbstraction fs;
    private final Log log;

    ClusterStateCleaner( LocalDatabase localDatabase, ClusterStateDirectory clusterStateDirectory, FileSystemAbstraction fs, LogProvider logProvider )
    {
        this.localDatabase = localDatabase;
        this.clusterStateDirectory = clusterStateDirectory;
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
        return localDatabase.isEmpty() && !clusterStateDirectory.isEmpty();
    }
}
