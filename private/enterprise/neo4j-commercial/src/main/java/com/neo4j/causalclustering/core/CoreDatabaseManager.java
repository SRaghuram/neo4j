/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.causalclustering.common.ClusteredDatabaseContextFactory;
import com.neo4j.causalclustering.common.ClusteredMultiDatabaseManager;
import com.neo4j.causalclustering.core.state.CoreStateService;
import com.neo4j.causalclustering.core.state.DatabaseCoreStateComponents;

import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.monitoring.DatabaseHealth;

public class CoreDatabaseManager extends ClusteredMultiDatabaseManager<CoreDatabaseContext>
{

    private final Supplier<CoreStateService> coreStateService;

    //TODO: We can do better at untangling this than a coreState supplier here right?
    CoreDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, Logger log, GraphDatabaseFacade facade,
            Supplier<CoreStateService> coreStateService, AvailabilityGuard availabilityGuard, FileSystemAbstraction fs,
            PageCache pageCache, LogProvider logProvider, Config config, DatabaseHealth globalHealths )
    {
        super( globalModule, edition, log, facade, contextFactory( coreStateService ), fs, pageCache, logProvider, config, globalHealths, availabilityGuard );
        this.coreStateService = coreStateService;
    }

    private static ClusteredDatabaseContextFactory<CoreDatabaseContext> contextFactory( Supplier<CoreStateService> coreStateService )
    {
        return ( Database database, GraphDatabaseFacade facade, LogFiles txLogs,
                StoreFiles storeFiles, LogProvider logProvider, BooleanSupplier isAvailable ) ->
                new CoreDatabaseContext( database, facade, txLogs, storeFiles, logProvider, isAvailable, coreStateService.get() );
    }

    @Override
    protected CoreDatabaseContext createNewDatabaseContext( String databaseName )
    {
        DatabaseCoreStateComponents.LifecycleDependencies lifecycleDeps = new DatabaseCoreStateComponents.LifecycleDependencies();
        CommitProcessInstaller commitProcessInstaller = new CommitProcessInstaller();
        coreStateService.get().create( databaseName, lifecycleDeps, commitProcessInstaller );
        //The database requires several cluster specific versions of kernel machines, created by the CoreStateService. However, some of those components need
        // a reference to the database at runtime, after it has started. Hence this circular pattern.
        CoreDatabaseContext ctx = super.createNewDatabaseContext( databaseName );
        commitProcessInstaller.registerInstallListener( ctx::setCommitProcess );
        lifecycleDeps.inject( ctx.database() );
        return ctx;
    }

}
