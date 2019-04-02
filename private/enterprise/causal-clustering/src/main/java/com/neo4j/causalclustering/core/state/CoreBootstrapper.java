/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.common.ClusteredDatabaseManager;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.helper.TemporaryDatabaseFactory;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.factory.module.DatabaseInitializer;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;

public class CoreBootstrapper
{
    private final ClusteredDatabaseManager<?> databaseManager;
    private final TemporaryDatabaseFactory tempDatabaseFactory;
    private final Function<DatabaseId,DatabaseInitializer> databaseInitializers;
    private final PageCache pageCache;
    private final FileSystemAbstraction fs;
    private final LogProvider logProvider;
    private final StorageEngineFactory storageEngineFactory;
    private final Config config;

    CoreBootstrapper( ClusteredDatabaseManager<?> databaseManager, TemporaryDatabaseFactory tempDatabaseFactory,
            Function<DatabaseId,DatabaseInitializer> databaseInitializers, FileSystemAbstraction fs, Config config, LogProvider logProvider,
            PageCache pageCache, StorageEngineFactory storageEngineFactory )
    {
        this.databaseManager = databaseManager;
        this.tempDatabaseFactory = tempDatabaseFactory;
        this.databaseInitializers = databaseInitializers;
        this.fs = fs;
        this.pageCache = pageCache;
        this.logProvider = logProvider;
        this.config = config;
        this.storageEngineFactory = storageEngineFactory;
    }

    /**
     * Bootstraps the cluster using the supplied set of members.
     *
     * @param members the members to bootstrap with (this comes from the discovery service).
     * @return a snapshot which represents the initial state.
     */
    public Map<DatabaseId,CoreSnapshot> bootstrap( Set<MemberId> members )
    {
        DatabaseBootstrapper dbBootstrapper = new DatabaseBootstrapper( members, tempDatabaseFactory, databaseInitializers,
                pageCache, fs, logProvider, storageEngineFactory, config );

        Map<DatabaseId,CoreSnapshot> coreSnapshots = new HashMap<>();
        for ( ClusteredDatabaseContext dbContext : databaseManager.registeredDatabases().values() )
        {
            coreSnapshots.put( dbContext.databaseId(), dbBootstrapper.bootstrap( dbContext ) );
        }

        return coreSnapshots;
    }
}
