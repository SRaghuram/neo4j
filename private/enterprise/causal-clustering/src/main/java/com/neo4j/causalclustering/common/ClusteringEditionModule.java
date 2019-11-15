/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.kernel.impl.enterprise.EnterpriseConstraintSemantics;
import com.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import com.neo4j.kernel.impl.net.DefaultNetworkConnectionTracker;
import com.neo4j.kernel.impl.pagecache.PageCacheWarmer;

import java.util.function.Predicate;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.impl.BoltKernelDatabaseManagementServiceProvider;
import org.neo4j.bolt.txtracking.DefaultReconciledTransactionTracker;
import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.time.SystemNanoClock;

public abstract class ClusteringEditionModule extends AbstractEditionModule
{
    protected final ReconciledTransactionTracker reconciledTxTracker;

    protected ClusteringEditionModule( GlobalModule globalModule )
    {
        reconciledTxTracker = new DefaultReconciledTransactionTracker( globalModule.getLogService() );
    }

    protected void editionInvariants( GlobalModule globalModule, Dependencies dependencies )
    {
        ioLimiter = new ConfigurableIOLimiter( globalModule.getGlobalConfig() );

        constraintSemantics = new EnterpriseConstraintSemantics();

        connectionTracker = dependencies.satisfyDependency( createConnectionTracker() );
    }

    @Override
    protected NetworkConnectionTracker createConnectionTracker()
    {
        return new DefaultNetworkConnectionTracker();
    }

    public static Predicate<String> fileWatcherFileNameFilter()
    {
        return Predicates.any( fileName -> fileName.startsWith( TransactionLogFilesHelper.DEFAULT_NAME ),
                filename -> filename.endsWith( PageCacheWarmer.SUFFIX_CACHEPROF ) );
    }

    @Override
    public BoltGraphDatabaseManagementServiceSPI createBoltDatabaseManagementServiceProvider( Dependencies dependencies,
            DatabaseManagementService managementService, Monitors monitors, SystemNanoClock clock, LogService logService )
    {
        var config = dependencies.resolveDependency( Config.class );
        var bookmarkAwaitDuration =  config.get( GraphDatabaseSettings.bookmark_ready_timeout );
        return new BoltKernelDatabaseManagementServiceProvider( managementService, reconciledTxTracker, monitors, clock, bookmarkAwaitDuration );
    }
}
