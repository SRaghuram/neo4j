/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.DatabaseOperationCountMonitorListener;
import com.neo4j.dbms.database.DatabaseOperationCountMonitor;
import com.neo4j.dbms.database.MultiDatabaseManager;

import java.util.stream.Stream;

import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.extension.GlobalExtensions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.lifecycle.LifecycleListener;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.storageengine.api.TransactionIdStore;

import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.INITIAL;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

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

        this.globalModule = globalModule;
        this.databaseManager = databaseManager;
        this.databaseIdRepository = databaseManager.databaseIdRepository();
        this.localOperator = new LocalDbmsOperator( databaseIdRepository );
        this.reconciledTxTracker = reconciledTxTracker;
        this.systemOperator = new SystemGraphDbmsOperator( dbmsModel, reconciledTxTracker, internalLogProvider );
        this.shutdownOperator = new ShutdownOperator( databaseManager, globalModule.getGlobalConfig() );
        this.reconciler = reconciler;
        var databaseCountMonitor = new DatabaseOperationCountMonitorListener(
                globalModule.getGlobalMonitors().newMonitor( DatabaseOperationCountMonitor.class, getClass().getName() ) );
        reconciler.registerListener( databaseCountMonitor );
        globalModule.getGlobalLife().addLifecycleListener( createReconcilerMonitorResetListener( databaseCountMonitor ) );
        globalModule.getGlobalDependencies().satisfyDependency( reconciler );
        globalModule.getGlobalDependencies().satisfyDependencies( localOperator, systemOperator );
    }

    @Override
    public void start() throws Exception
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
    private void startInitialDatabases() throws DatabaseManagementException
    {
        // Initially trigger system operator to start system db, it always desires the system db to be STARTED
        systemOperator.trigger( ReconcilerRequest.simple() ).join( NAMED_SYSTEM_DATABASE_ID );

        var systemDatabase = getSystemDatabase( databaseManager );
        long lastClosedTxId = getLastClosedTransactionId( systemDatabase );

        // Manually kick off the reconciler to start all other databases in the system database, now that the system database is started
        systemOperator.updateDesiredStates();
        systemOperator.trigger( ReconcilerRequest.simple() ).awaitAll();

        // More state changes might have been committed into the system database and been reconciled before
        // the following call, and those will independently and concurrently call offerReconciledTransactionId
        // from the transactional threads.
        reconciledTxTracker.enable( lastClosedTxId );
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

    /**
     * This method defines the table mapping any pair of database states to the series of steps the reconciler needs to perform
     * to take a database from one state to another.
     */
    static TransitionsTable createTransitionsTable( ReconcilerTransitions t )
    {
        return TransitionsTable.builder()
                .from( INITIAL ).to( DROPPED ).doNothing()
                .from( INITIAL ).to( STOPPED ).doTransitions( t.create() )
                .from( INITIAL ).to( STARTED ).doTransitions( t.create(), t.start() )
                .from( STOPPED ).to( STARTED ).doTransitions( t.start() )
                .from( STARTED ).to( STOPPED ).doTransitions( t.stop() )
                .from( STOPPED ).to( DROPPED ).doTransitions( t.drop() )
                .from( STARTED ).to( DROPPED ).doTransitions( t.prepareDrop(), t.stop(), t.drop() )
                .build();
    }

    private GraphDatabaseAPI getSystemDatabase( MultiDatabaseManager<?> databaseManager )
    {
        return databaseManager.getDatabaseContext( NAMED_SYSTEM_DATABASE_ID ).orElseThrow().databaseFacade();
    }

    private static DbmsReconciler createReconciler( GlobalModule globalModule, MultiDatabaseManager<?> databaseManager )
    {
        var transitionsTable = createTransitionsTable( new ReconcilerTransitions( databaseManager ) );
        return new DbmsReconciler( databaseManager, globalModule.getGlobalConfig(), globalModule.getLogService().getInternalLogProvider(),
                globalModule.getJobScheduler(), transitionsTable );
    }

    private long getLastClosedTransactionId( GraphDatabaseAPI db )
    {
        var resolver = db.getDependencyResolver();
        var txIdStore = resolver.resolveDependency( TransactionIdStore.class );
        return txIdStore.getLastClosedTransactionId();
    }

    private LifecycleListener createReconcilerMonitorResetListener( DatabaseOperationCountMonitorListener databaseOperationCountMonitorListener )
    {
        return ( instance, from, to ) ->
        {
            if ( instance instanceof GlobalExtensions && to.equals( LifecycleStatus.STARTED ) )
            {
                databaseOperationCountMonitorListener.reset( reconciler.statesSnapshot() );
            }
        };
    }

}
