/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.core.state.CoreLife;
import com.neo4j.kernel.impl.enterprise.CommercialConstraintSemantics;
import com.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import com.neo4j.kernel.impl.net.DefaultNetworkConnectionTracker;
import com.neo4j.kernel.impl.pagecache.PageCacheWarmer;

import java.io.File;
import java.util.function.Predicate;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.udc.UsageData;

public abstract class ClusteringEditionModule extends AbstractEditionModule
{

    protected void editionInvariants( GlobalModule globalModule, Dependencies dependencies, Config config, LifeSupport life )
    {
        KernelData kernelData = createKernelData( globalModule.getFileSystem(), globalModule.getPageCache(),
                                                globalModule.getStoreLayout().storeDirectory(), config );
        dependencies.satisfyDependency( kernelData );
        life.add( kernelData );

        ioLimiter = new ConfigurableIOLimiter( globalModule.getGlobalConfig() );

        headerInformationFactory = createHeaderInformationFactory();

        schemaWriteGuard = createSchemaWriteGuard();

        transactionStartTimeout = config.get( GraphDatabaseSettings.transaction_start_timeout ).toMillis();

        constraintSemantics = new CommercialConstraintSemantics();

        publishEditionInfo( dependencies.resolveDependency( UsageData.class ), globalModule.getDatabaseInfo(), config );

        connectionTracker = dependencies.satisfyDependency( createConnectionTracker() );
    }

    protected abstract SchemaWriteGuard createSchemaWriteGuard();

    private static KernelData createKernelData( FileSystemAbstraction fileSystem, PageCache pageCache, File storeDir, Config config )
    {
        return new KernelData( fileSystem, pageCache, storeDir, config );
    }

    private TransactionHeaderInformationFactory createHeaderInformationFactory()
    {
        return () -> new TransactionHeaderInformation( -1, -1, new byte[0] );
    }

    @Override
    protected NetworkConnectionTracker createConnectionTracker()
    {
        return new DefaultNetworkConnectionTracker();
    }

    public static Predicate<String> fileWatcherFileNameFilter()
    {
        return Predicates.any( fileName -> fileName.startsWith( TransactionLogFiles.DEFAULT_NAME ),
                filename -> filename.endsWith( PageCacheWarmer.SUFFIX_CACHEPROF ) );
    }

    /**
     * Returns {@code true} because {@link DatabaseManager}'s lifecycle is managed by {@link DatabaseService} via {@link CoreLife} and
     * read replica startup process. So {@link DatabaseManager} does not need to be included in the global lifecycle.
     *
     * @return always {@code true}.
     */
    @Override
    public final boolean handlesDatabaseManagerLifecycle()
    {
        return true;
    }
}
