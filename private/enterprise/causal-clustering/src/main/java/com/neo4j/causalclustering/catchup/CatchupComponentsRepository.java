/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyProcess;
import com.neo4j.causalclustering.common.ClusteredDatabaseContext;

import java.util.Optional;

import org.neo4j.io.layout.DatabaseLayout;

/**
 * The components needed to perform store copy and catchup operations for databases in Neo4j.
 *
 * For each database on this machine, there is a {@link ClusteredDatabaseContext}, a {@link RemoteStore}
 * and a {@link StoreCopyProcess}.
 *
 * The {@link ClusteredDatabaseContext} instance may be used to start/stop a database, as well as perform file system operations via {@link DatabaseLayout}.
 * The {@link RemoteStore} instance may be used to catchup, via transaction pulling, to a remote machine.
 * The {@link StoreCopyProcess} instance may be used to catchup, via store copying, to a remote machine.
 */
public interface CatchupComponentsRepository
{
    Optional<DatabaseCatchupComponents> componentsFor( String databaseName );

    /** Simple struct to make working with various per database catchup components a bit easier */
    class DatabaseCatchupComponents
    {
        final RemoteStore remoteStore;
        final StoreCopyProcess storeCopy;

        public DatabaseCatchupComponents( RemoteStore remoteStore, StoreCopyProcess storeCopy )
        {
            this.remoteStore = remoteStore;
            this.storeCopy = storeCopy;
        }

        public StoreCopyProcess storeCopyProcess()
        {
            return storeCopy;
        }

        public RemoteStore remoteStore()
        {
            return remoteStore;
        }
    }
}
