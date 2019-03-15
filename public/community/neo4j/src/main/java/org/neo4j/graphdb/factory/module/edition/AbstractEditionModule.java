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

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dmbs.database.DefaultDatabaseManager;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseContext;
import org.neo4j.internal.collector.DataCollectorProcedures;
import org.neo4j.io.fs.watcher.DatabaseLayoutWatcher;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.impl.fulltext.FulltextProcedures;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.transaction.stats.GlobalTransactionStats;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;
import org.neo4j.kernel.impl.util.watcher.DefaultFileDeletionListenerFactory;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;
import org.neo4j.logging.internal.LogService;
import org.neo4j.procedure.builtin.BuiltInDbmsProcedures;
import org.neo4j.procedure.builtin.BuiltInFunctions;
import org.neo4j.procedure.builtin.BuiltInProcedures;
import org.neo4j.procedure.builtin.TokenProcedures;
import org.neo4j.procedure.impl.ProcedureConfig;
import org.neo4j.service.Services;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;

import static org.neo4j.procedure.impl.temporal.TemporalFunction.registerTemporalFunctions;

/**
 * Edition module for {@link GraphDatabaseFacadeFactory}. Implementations of this class
 * need to create all the services that would be specific for a particular edition of the database.
 */
public abstract class AbstractEditionModule
{
    private final GlobalTransactionStats transactionStatistic = new GlobalTransactionStats();
    protected NetworkConnectionTracker connectionTracker;
    protected ThreadToStatementContextBridge threadToTransactionBridge;
    protected long transactionStartTimeout;
    protected TransactionHeaderInformationFactory headerInformationFactory;
    protected SchemaWriteGuard schemaWriteGuard;
    protected ConstraintSemantics constraintSemantics;
    protected AccessCapability accessCapability;
    protected IOLimiter ioLimiter;
    protected Function<DatabaseLayout,DatabaseLayoutWatcher> watcherServiceFactory;
    protected SecurityProvider securityProvider;

    public abstract EditionDatabaseContext createDatabaseContext( String databaseName );

    protected DatabaseLayoutWatcher createDatabaseFileSystemWatcher( FileWatcher watcher, DatabaseLayout databaseLayout, LogService logging,
            Predicate<String> fileNameFilter )
    {
        DefaultFileDeletionListenerFactory listenerFactory =
                new DefaultFileDeletionListenerFactory( databaseLayout.getDatabaseName(), logging, fileNameFilter );
        return new DatabaseLayoutWatcher( watcher, databaseLayout, listenerFactory );
    }

    public void registerProcedures( GlobalProcedures globalProcedures, ProcedureConfig procedureConfig ) throws KernelException
    {
        globalProcedures.registerProcedure( BuiltInProcedures.class );
        globalProcedures.registerProcedure( TokenProcedures.class );
        globalProcedures.registerProcedure( BuiltInDbmsProcedures.class );
        globalProcedures.registerProcedure( FulltextProcedures.class );
        globalProcedures.registerProcedure( DataCollectorProcedures.class );
        globalProcedures.registerBuiltInFunctions( BuiltInFunctions.class );
        registerTemporalFunctions( globalProcedures, procedureConfig );

        registerEditionSpecificProcedures( globalProcedures );
    }

    protected abstract void registerEditionSpecificProcedures( GlobalProcedures globalProcedures ) throws KernelException;

    protected void publishEditionInfo( UsageData sysInfo, DatabaseInfo databaseInfo, Config config )
    {
        sysInfo.set( UsageDataKeys.edition, databaseInfo.edition );
        sysInfo.set( UsageDataKeys.operationalMode, databaseInfo.operationalMode );
        config.augment( GraphDatabaseSettings.editionName, databaseInfo.edition.toString() );
    }

    public DatabaseManager createDatabaseManager( GraphDatabaseFacade graphDatabaseFacade, GlobalModule globalModule, AbstractEditionModule edition,
            GlobalProcedures globalProcedures, Logger msgLog )
    {
        return new DefaultDatabaseManager( globalModule, edition, globalProcedures, msgLog, graphDatabaseFacade );
    }

    /**
     * Returns {@code false} because {@link DatabaseManager}'s lifecycle is not managed by any component by default.
     * So {@link DatabaseManager} needs to be included in the global lifecycle.
     *
     * @return always {@code false}.
     */
    public boolean handlesDatabaseManagerLifecycle()
    {
        return false;
    }

    public abstract void createSecurityModule( GlobalModule globalModule, GlobalProcedures globalProcedures );

    protected static SecurityModule setupSecurityModule( GlobalModule globalModule, AbstractEditionModule editionModule, Log log,
            GlobalProcedures globalProcedures, String key )
    {
        SecurityModule.Dependencies securityModuleDependencies = new SecurityModuleDependencies( globalModule, editionModule, globalProcedures );
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
            log.error( errorMessage );
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

    public void createDatabases( DatabaseManager databaseManager, Config config )
    {
        databaseManager.createDatabase( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
        databaseManager.createDatabase( config.get( GraphDatabaseSettings.default_database ) );
    }

    public long getTransactionStartTimeout()
    {
        return transactionStartTimeout;
    }

    public SchemaWriteGuard getSchemaWriteGuard()
    {
        return schemaWriteGuard;
    }

    public TransactionHeaderInformationFactory getHeaderInformationFactory()
    {
        return headerInformationFactory;
    }

    public ConstraintSemantics getConstraintSemantics()
    {
        return constraintSemantics;
    }

    public IOLimiter getIoLimiter()
    {
        return ioLimiter;
    }

    public AccessCapability getAccessCapability()
    {
        return accessCapability;
    }

    public Function<DatabaseLayout,DatabaseLayoutWatcher> getWatcherServiceFactory()
    {
        return watcherServiceFactory;
    }

    public ThreadToStatementContextBridge getThreadToTransactionBridge()
    {
        return threadToTransactionBridge;
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
}
