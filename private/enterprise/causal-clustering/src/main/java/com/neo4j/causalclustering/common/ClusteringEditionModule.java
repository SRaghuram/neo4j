/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.dbms.ClusterDbmsRuntimeRepository;
import com.neo4j.dbms.ReplicatedDatabaseEventService;
import com.neo4j.kernel.impl.enterprise.EnterpriseConstraintSemantics;
import com.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.ConfigurableIOLimiter;
import com.neo4j.kernel.impl.pagecache.PageCacheWarmer;

import java.util.Set;
import java.util.function.Predicate;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.impl.BoltKernelDatabaseManagementServiceProvider;
import org.neo4j.bolt.runtime.scheduling.NettyThreadFactory;
import org.neo4j.bolt.txtracking.DefaultReconciledTransactionTracker;
import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DatabaseOperationCounts;
import org.neo4j.dbms.database.DbmsRuntimeRepository;
import org.neo4j.dbms.database.DbmsRuntimeSystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.SystemNanoClock;

import static com.neo4j.configuration.CausalClusteringSettings.store_copy_parallelism;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;
import static org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper.DEFAULT_FILENAME_PREDICATE;

public abstract class ClusteringEditionModule extends AbstractEditionModule
{
    private static final Set<Group> NETTY_THREAD_GROUPS = Set.of( Group.CATCHUP_CLIENT, Group.CATCHUP_SERVER, Group.RAFT_CLIENT, Group.RAFT_SERVER );

    protected final ReconciledTransactionTracker reconciledTxTracker;

    protected ClusteringEditionModule( GlobalModule globalModule )
    {
        reconciledTxTracker = new DefaultReconciledTransactionTracker( globalModule.getLogService() );
        configureThreadFactories( globalModule.getJobScheduler() );
        globalModule.getGlobalDependencies().satisfyDependency( new DatabaseOperationCounts.Counter() ); // for global metrics and multidbmanager
        setGlobalStoreCopyParallelism( globalModule );
    }

    private void setGlobalStoreCopyParallelism( GlobalModule globalModule )
    {
        final var config = globalModule.getGlobalConfig();
        final var storeCopyParallelism = config.get( store_copy_parallelism );
        globalModule.getJobScheduler().setParallelism( Group.STORE_COPY_CLIENT, storeCopyParallelism );
    }

    protected void editionInvariants( GlobalModule globalModule, Dependencies dependencies )
    {
        ioLimiter = new ConfigurableIOLimiter( globalModule.getGlobalConfig() );

        constraintSemantics = new EnterpriseConstraintSemantics();

        connectionTracker = dependencies.satisfyDependency( createConnectionTracker() );
    }

    public static Predicate<String> fileWatcherFileNameFilter()
    {
        return Predicates.any( DEFAULT_FILENAME_PREDICATE,
                filename -> filename.endsWith( PageCacheWarmer.SUFFIX_CACHEPROF ) );
    }

    @Override
    public SystemGraphInitializer createSystemGraphInitializer( GlobalModule globalModule )
    {
        return globalModule.getGlobalDependencies().satisfyDependency( SystemGraphInitializer.NO_OP );
    }

    @Override
    public BoltGraphDatabaseManagementServiceSPI createBoltDatabaseManagementServiceProvider( Dependencies dependencies,
            DatabaseManagementService managementService, Monitors monitors, SystemNanoClock clock, LogService logService )
    {
        var config = dependencies.resolveDependency( Config.class );
        var bookmarkAwaitDuration = config.get( GraphDatabaseSettings.bookmark_ready_timeout );
        return new BoltKernelDatabaseManagementServiceProvider( managementService, monitors, clock, bookmarkAwaitDuration );
    }

    private static void configureThreadFactories( JobScheduler jobScheduler )
    {
        for ( var group : NETTY_THREAD_GROUPS )
        {
            // thread factories can only be configured once, before the group's executor is started
            jobScheduler.setThreadFactory( group, NettyThreadFactory::new );
        }
    }

    @Override
    public DbmsRuntimeRepository createAndRegisterDbmsRuntimeRepository( GlobalModule globalModule, DatabaseManager<?> databaseManager,
            Dependencies dependencies, DbmsRuntimeSystemGraphComponent dbmsRuntimeSystemGraphComponent )
    {
        var dbmsRuntimeRepository = new ClusterDbmsRuntimeRepository( databaseManager, dbmsRuntimeSystemGraphComponent );
        var replicatedDatabaseEventService = dependencies.resolveDependency( ReplicatedDatabaseEventService.class );
        replicatedDatabaseEventService.registerListener( NAMED_SYSTEM_DATABASE_ID, dbmsRuntimeRepository );
        return dbmsRuntimeRepository;
    }
}
