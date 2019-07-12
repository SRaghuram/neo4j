/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.causalclustering.common.ClusteredMultiDatabaseManager;

import java.util.stream.Stream;

import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.kernel.database.DatabaseIdRepository;

public class ClusteredDbmsReconcilerModule extends StandaloneDbmsReconcilerModule<ClusteredMultiDatabaseManager>
{
    private final TransactionEventService txEventService;
    private final ClusterInternalDbmsOperator internalOperator;

    public ClusteredDbmsReconcilerModule( GlobalModule globalModule, ClusteredMultiDatabaseManager databaseManager, TransactionEventService txEventService,
            ClusterInternalDbmsOperator internalOperator, DatabaseIdRepository databaseIdRepository )
    {
        super( globalModule, databaseManager, databaseIdRepository );
        this.txEventService = txEventService;
        this.internalOperator = internalOperator;
        //TODO: don't need if we do end up injecting
        globalModule.getGlobalDependencies().satisfyDependencies( internalOperator );
    }

    @Override
    protected Stream<DbmsOperator> operators()
    {
        return Stream.concat( super.operators(), Stream.of( internalOperator ) );
    }

    @Override
    protected void registerWithListenerService( GlobalModule globalModule, SystemGraphDbmsOperator systemOperator )
    {
        txEventService.registerHandler( databaseIdRepository.systemDatabase(), txId -> systemOperator.transactionCommitted( txId, null ) );
    }

    @Override
    protected ClusteredDbmsReconciler createReconciler( GlobalModule globalModule, ClusteredMultiDatabaseManager databaseManager )
    {
        return new ClusteredDbmsReconciler( databaseManager, globalModule.getGlobalConfig(), globalModule.getLogService().getInternalLogProvider(),
                globalModule.getJobScheduler() );
    }
}
