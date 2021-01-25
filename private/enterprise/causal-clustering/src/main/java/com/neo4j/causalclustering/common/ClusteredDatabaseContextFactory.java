/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.catchup.CatchupComponentsFactory;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.dbms.database.ClusteredDatabaseContext;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;

@FunctionalInterface
public interface ClusteredDatabaseContextFactory
{
    ClusteredDatabaseContext create( Database database, GraphDatabaseFacade facade, LogFiles txLogs, StoreFiles storeFiles,
            LogProvider logProvider, CatchupComponentsFactory factory, ClusteredDatabase clusterDatabase, Monitors clusterDatabaseMonitors,
            PageCacheTracer cacheTracer );
}
