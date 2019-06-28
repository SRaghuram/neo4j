/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.MultiDatabaseManager;

import java.util.stream.Stream;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class StandaloneDbmsReconcilerModule<DM extends MultiDatabaseManager<? extends DatabaseContext>> extends LifecycleAdapter
{
    private final GlobalModule globalModule;
    private final DM databaseManager;
    private final LocalDbmsOperator localOperator;
    private final SystemGraphDbmsModel dbmsModel;
    private final SystemGraphDbmsOperator systemOperator;
    private final ShutdownOperator shutdownOperator;
    protected final DatabaseIdRepository databaseIdRepository;

    public StandaloneDbmsReconcilerModule( GlobalModule globalModule, DM databaseManager, DatabaseIdRepository databaseIdRepository )
    {
        this.globalModule = globalModule;
        this.databaseManager = databaseManager;
        this.databaseIdRepository = databaseIdRepository;
        this.localOperator = new LocalDbmsOperator( databaseIdRepository );

        this.dbmsModel = new SystemGraphDbmsModel( databaseIdRepository );
        this.systemOperator = new SystemGraphDbmsOperator( dbmsModel, databaseIdRepository );
        this.shutdownOperator = new ShutdownOperator( databaseManager );
        globalModule.getGlobalDependencies().satisfyDependencies( localOperator, systemOperator );
    }

    @Override
    public void start()
    {
        registerWithListenerService( globalModule, systemOperator );
        DbmsReconciler reconciler = createReconciler( globalModule, databaseManager );

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
        systemOperator.trigger().await( databaseIdRepository.systemDatabase() );

        GraphDatabaseService systemDatabase = getSystemDatabase( databaseManager );
        dbmsModel.setSystemDatabase( systemDatabase );

        //Manually kick off the reconciler to start all other databases in the system database, now that the system database is started
        systemOperator.updateDesiredStates();
        systemOperator.trigger().awaitAll();
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
                systemOperator.transactionCommitted( txData.getTransactionId(), dbmsModel.updatedDatabases( txData ) );
            }
        } );
    }

    private GraphDatabaseFacade getSystemDatabase( DM databaseManager )
    {
        return databaseManager.getDatabaseContext( databaseIdRepository.systemDatabase() ).orElseThrow().databaseFacade();
    }

    protected DbmsReconciler createReconciler( GlobalModule globalModule, DM databaseManager )
    {
        return new DbmsReconciler( databaseManager, globalModule.getGlobalConfig(), globalModule.getLogService().getInternalLogProvider(),
                globalModule.getJobScheduler() );
    }
}
