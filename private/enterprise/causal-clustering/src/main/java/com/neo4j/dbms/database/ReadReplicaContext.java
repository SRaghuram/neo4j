/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import com.neo4j.causalclustering.catchup.CatchupComponentsFactory;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.causalclustering.common.ClusteredDatabase;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;

public class ReadReplicaContext extends DefaultClusteredDatabaseContext
{

    public ReadReplicaContext( Database database, GraphDatabaseFacade facade, LogFiles txLogs, StoreFiles storeFiles, LogProvider logProvider,
            CatchupComponentsFactory catchupComponentsFactory, ClusteredDatabase clusterDatabase, Monitors clusterDatabaseMonitors,
            LeaderLocator leaderLocator, PageCacheTracer cacheTracer )
    {
        super( database, facade, txLogs, storeFiles, logProvider, catchupComponentsFactory,
               clusterDatabase, clusterDatabaseMonitors, leaderLocator, cacheTracer );
    }
}
