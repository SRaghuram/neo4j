/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.MultiDatabaseManager;

import java.util.stream.Stream;

import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.storageengine.api.TransactionIdStore;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;

public class StandaloneDbmsReconcilerModule extends LifecycleAdapter
{
    private final GlobalModule globalModule;
    private final MultiDatabaseManager<?> databaseManager;
    private final LocalDbmsOperator localOperator;
    private final SystemGraphDbmsOperator systemOperator;
    private final ShutdownOperator shutdownOperator;
    protected final DatabaseIdRepository databaseIdRepository;
    private final ReconciledTransactionTracker reconciledTxTracker;
    private final DbmsReconciler reconciler;

    public StandaloneDbmsReconcilerModule( GlobalModule globalModule, MultiDatabaseManager<?> databaseManager, ReconciledTransactionTracker reconciledTxTracker,
            EnterpriseSystemGraphDbmsModel dbmsModel )
    {
        this( globalModule, databaseManager, reconciledTxTracker, createReconciler( globalModule, databaseManager ), dbmsModel );
    }

    protected StandaloneDbmsReconcilerModule( GlobalModule globalModule, MultiDatabaseManager<?> databaseManager,
            ReconciledTransactionTracker reconciledTxTracker, DbmsReconciler reconciler, EnterpriseSystemGraphDbmsModel dbmsModel )
    {
        var internalLogProvider = globalModule.getLogService().getInternalLogProvider();
        var txBridge = globalModule.getThreadToTransactionBridge();

        this.globalModule = globalModule;
        this.databaseManager = databaseManager;
        this.databaseIdRepository = databaseManager.databaseIdRepository();
        this.localOperator = new LocalDbmsOperator( databaseIdRepository );
        this.reconciledTxTracker = reconciledTxTracker;
        this.systemOperator = new SystemGraphDbmsOperator( dbmsModel, txBridge, reconciledTxTracker, internalLogProvider );
        this.shutdownOperator = new ShutdownOperator( databaseManager );
        this.reconciler = reconciler;
        globalModule.getGlobalDependencies().satisfyDependency( reconciler );
        globalModule.getGlobalDependencies().satisfyDependencies( localOperator, systemOperator );
    }

    @Override
    public void start()
    {
        registerWithListenerService( globalModule, systemOperator );
        var connector = new OperatorConnector( reconciler );
        operators().forEach( op -> op.connect( connector ) );
        startInitialDatabases();
    }

    /**
     * Blocking call. Just syntactic sugar around a trigger. Used to transition default databases to
     * desired initial states at DatabaseManager startup
     */
    private void startInitialDatabases()
    {
        // Initially trigger system operator to start system db, it always desires the system db to be STARTED
        systemOperator.trigger( ReconcilerRequest.simple() ).await( SYSTEM_DATABASE_ID );

        // Manually kick off the reconciler to start all other databases in the system database, now that the system database is started
        systemOperator.updateDesiredStates();
        systemOperator.trigger( ReconcilerRequest.simple() ).awaitAll();

        var systemDatabase = getSystemDatabase( databaseManager );
        initializeReconciledTxTracker( systemDatabase );
    }

    /**
     * Blocking call. Transitions the desired states of all non-dropped databases to stopped.
     * Used at DatabaseManager stop.
     */
    private void stopAllDatabases()
    {
        shutdownOperator.stopAll();
    }

    @Override
    public void stop()
    {
        stopAllDatabases();
    }

    public DbmsReconciler reconciler()
    {
        return reconciler;
    }

    protected Stream<DbmsOperator> operators()
    {
        return Stream.of( localOperator, systemOperator, shutdownOperator );
    }

    protected void registerWithListenerService( GlobalModule globalModule, SystemGraphDbmsOperator systemOperator )
    {
        globalModule.getTransactionEventListeners().registerTransactionEventListener( SYSTEM_DATABASE_NAME, new TransactionEventListenerAdapter()
        {
            @Override
            public void afterCommit( TransactionData txData, Object state, GraphDatabaseService systemDatabase )
            {
                systemOperator.transactionCommitted( txData.getTransactionId(), txData );
            }
        } );
    }

    private GraphDatabaseAPI getSystemDatabase( MultiDatabaseManager<?> databaseManager )
    {
        return databaseManager.getDatabaseContext( SYSTEM_DATABASE_ID ).orElseThrow().databaseFacade();
    }

    private static DbmsReconciler createReconciler( GlobalModule globalModule, MultiDatabaseManager<?> databaseManager )
    {
        return new DbmsReconciler( databaseManager, globalModule.getGlobalConfig(), globalModule.getLogService().getInternalLogProvider(),
                globalModule.getJobScheduler() );
    }

    private void initializeReconciledTxTracker( GraphDatabaseAPI systemDb )
    {
        var resolver = systemDb.getDependencyResolver();
        var txIdStore = resolver.resolveDependency( TransactionIdStore.class );
        reconciledTxTracker.initialize( txIdStore.getLastClosedTransactionId() );
    }

}
