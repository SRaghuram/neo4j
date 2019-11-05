/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphdb.factory.module.edition;

import java.util.function.Function;
import java.util.function.Predicate;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseComponents;
import org.neo4j.internal.collector.DataCollectorProcedures;
import org.neo4j.io.fs.watcher.DatabaseLayoutWatcher;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.transaction.stats.GlobalTransactionStats;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;
import org.neo4j.kernel.impl.util.watcher.DefaultFileDeletionListenerFactory;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.procedure.builtin.BuiltInDbmsProcedures;
import org.neo4j.procedure.builtin.BuiltInFunctions;
import org.neo4j.procedure.builtin.BuiltInProcedures;
import org.neo4j.procedure.builtin.FulltextProcedures;
import org.neo4j.procedure.builtin.TokenProcedures;
import org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller;
import org.neo4j.procedure.impl.ProcedureConfig;
import org.neo4j.service.Services;
import org.neo4j.time.SystemNanoClock;

import static org.neo4j.procedure.impl.temporal.TemporalFunction.registerTemporalFunctions;

/**
 * Edition module for {@link DatabaseManagementServiceFactory}. Implementations of this class
 * need to create all the services that would be specific for a particular edition of the database.
 */
public abstract class AbstractEditionModule
{
    private final GlobalTransactionStats transactionStatistic = new GlobalTransactionStats();
    protected NetworkConnectionTracker connectionTracker;
    protected ConstraintSemantics constraintSemantics;
    protected IOLimiter ioLimiter;
    protected Function<DatabaseLayout,DatabaseLayoutWatcher> watcherServiceFactory;
    protected SecurityProvider securityProvider;
    protected GlobalProcedures globalProcedures;

    public abstract EditionDatabaseComponents createDatabaseComponents( DatabaseId databaseId );

    protected DatabaseLayoutWatcher createDatabaseFileSystemWatcher( FileWatcher watcher, DatabaseLayout databaseLayout, LogService logging,
            Predicate<String> fileNameFilter )
    {
        DefaultFileDeletionListenerFactory listenerFactory =
                new DefaultFileDeletionListenerFactory( new NormalizedDatabaseName( databaseLayout.getDatabaseName() ), logging, fileNameFilter );
        return new DatabaseLayoutWatcher( watcher, databaseLayout, listenerFactory );
    }

    public void registerProcedures( GlobalProcedures globalProcedures, ProcedureConfig procedureConfig, GlobalModule globalModule,
            DatabaseManager<?> databaseManager ) throws KernelException
    {
        globalProcedures.registerProcedure( BuiltInProcedures.class );
        globalProcedures.registerProcedure( TokenProcedures.class );
        globalProcedures.registerProcedure( BuiltInDbmsProcedures.class );
        globalProcedures.registerProcedure( FulltextProcedures.class );
        globalProcedures.registerProcedure( DataCollectorProcedures.class );
        globalProcedures.registerBuiltInFunctions( BuiltInFunctions.class );
        registerTemporalFunctions( globalProcedures, procedureConfig );

        registerEditionSpecificProcedures( globalProcedures, databaseManager );
        BaseRoutingProcedureInstaller routingProcedureInstaller = createRoutingProcedureInstaller( globalModule, databaseManager );
        routingProcedureInstaller.install( globalProcedures );
        this.globalProcedures = globalProcedures;
    }

    public GlobalProcedures getGlobalProcedures()
    {
        return globalProcedures;
    }

    protected abstract void registerEditionSpecificProcedures( GlobalProcedures globalProcedures, DatabaseManager<?> databaseManager )
            throws KernelException;

    protected abstract BaseRoutingProcedureInstaller createRoutingProcedureInstaller( GlobalModule globalModule, DatabaseManager<?> databaseManager );

    public abstract DatabaseManager<?> createDatabaseManager( GlobalModule globalModule );

    public abstract SystemGraphInitializer createSystemGraphInitializer( GlobalModule globalModule, DatabaseManager<?> databaseManager );

    public abstract void createSecurityModule( GlobalModule globalModule );

    protected static SecurityModule setupSecurityModule( GlobalModule globalModule, Log log, GlobalProcedures globalProcedures, String key )
    {
        SecurityModule.Dependencies securityModuleDependencies = new SecurityModuleDependencies( globalModule, globalProcedures );
        SecurityModule securityModule = Services.load( SecurityModule.class, key )
                .orElseThrow( () ->
                {
                    String errorMessage = "Failed to load security module with key '" + key + "'.";
                    log.error( errorMessage );
                    return new IllegalArgumentException( errorMessage );
                } );
        try
        {
            securityModule.setup( securityModuleDependencies );
            return securityModule;
        }
        catch ( Exception e )
        {
            String errorMessage = "Failed to load security module.";
            String innerErrorMessage = e.getMessage();

            if ( innerErrorMessage != null )
            {
                log.error( errorMessage + " Caused by: " + innerErrorMessage, e );
            }
            else
            {
                log.error( errorMessage, e );
            }
            throw new RuntimeException( errorMessage, e );
        }
    }

    protected NetworkConnectionTracker createConnectionTracker()
    {
        return NetworkConnectionTracker.NO_OP;
    }

    public DatabaseTransactionStats createTransactionMonitor()
    {
        return transactionStatistic.createDatabaseTransactionMonitor();
    }

    public TransactionCounters globalTransactionCounter()
    {
        return transactionStatistic;
    }

    public ConstraintSemantics getConstraintSemantics()
    {
        return constraintSemantics;
    }

    public IOLimiter getIoLimiter()
    {
        return ioLimiter;
    }

    public Function<DatabaseLayout,DatabaseLayoutWatcher> getWatcherServiceFactory()
    {
        return watcherServiceFactory;
    }

    public NetworkConnectionTracker getConnectionTracker()
    {
        return connectionTracker;
    }

    public SecurityProvider getSecurityProvider()
    {
        return securityProvider;
    }

    public void setSecurityProvider( SecurityProvider securityProvider )
    {
        this.securityProvider = securityProvider;
    }

    /**
     * @return the query engine provider for this edition.
     */
    public abstract QueryEngineProvider getQueryEngineProvider();

    public abstract BoltGraphDatabaseManagementServiceSPI createBoltDatabaseManagementServiceProvider( Dependencies dependencies,
            DatabaseManagementService managementService, Monitors monitors, SystemNanoClock clock, LogService logService );

    public AuthManager getBoltAuthManager( DependencyResolver dependencyResolver )
    {
        return dependencyResolver.resolveDependency( AuthManager.class );
    }
}
