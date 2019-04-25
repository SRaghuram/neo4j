/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.catchup.CatchupComponentsFactory;
import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.common.ClusteredMultiDatabaseManager;
import com.neo4j.causalclustering.core.state.CoreStateService;
import com.neo4j.causalclustering.core.state.DatabaseCoreStateComponents;

import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Health;

public class CoreDatabaseManager extends ClusteredMultiDatabaseManager
{
    private final Supplier<CoreStateService> coreStateService;

    CoreDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, Log log, GraphDatabaseFacade facade,
            Supplier<CoreStateService> coreStateService, CatchupComponentsFactory catchupComponentsFactory,
            AvailabilityGuard availabilityGuard, FileSystemAbstraction fs, PageCache pageCache,
            LogProvider logProvider, Config config, Health globalHealths )
    {
        super( globalModule, edition, log, facade, catchupComponentsFactory, fs, pageCache, logProvider, config, globalHealths, availabilityGuard );
        this.coreStateService = coreStateService;
    }

    @Override
    protected ClusteredDatabaseContext createNewDatabaseContext( DatabaseId databaseId )
    {
        var lifecycleDeps = new DatabaseCoreStateComponents.LifecycleDependencies();
        coreStateService.get().create( databaseId, lifecycleDeps );
        //The database requires several cluster specific versions of kernel machines, created by the CoreStateService. However, some of those components need
        // a reference to the database at runtime, after it has started. Hence this circular pattern.
        var ctx = super.createNewDatabaseContext( databaseId );
        lifecycleDeps.inject( ctx.database() );
        return ctx;
    }

    @Override
    protected void stopDatabase( DatabaseId databaseId, ClusteredDatabaseContext context, boolean storeCopying )
    {
        if ( !storeCopying )
        {
            coreStateService.get().remove( databaseId );
        }
        super.stopDatabase( databaseId, context, storeCopying );
    }
}
