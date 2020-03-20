/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.transaction;

import com.neo4j.fabric.bookmark.TransactionBookmarkManager;
import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.executor.FabricLocalExecutor;
import com.neo4j.fabric.executor.FabricRemoteExecutor;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;

public class TransactionManager extends LifecycleAdapter
{

    private final FabricRemoteExecutor remoteExecutor;
    private final FabricLocalExecutor localExecutor;
    private final LogService logService;
    private final JobScheduler jobScheduler;
    private final FabricConfig fabricConfig;

    private final Set<FabricTransactionImpl> openTransactions = ConcurrentHashMap.newKeySet();

    public TransactionManager( DependencyResolver dependencyResolver )
    {
        remoteExecutor = dependencyResolver.resolveDependency( FabricRemoteExecutor.class );
        localExecutor = dependencyResolver.resolveDependency( FabricLocalExecutor.class );
        logService = dependencyResolver.resolveDependency( LogService.class );
        jobScheduler = dependencyResolver.resolveDependency( JobScheduler.class );
        fabricConfig = dependencyResolver.resolveDependency( FabricConfig.class );
    }

    public FabricTransaction begin( FabricTransactionInfo transactionInfo, TransactionBookmarkManager transactionBookmarkManager )
    {
        transactionInfo.getLoginContext().authorize( LoginContext.IdLookup.EMPTY, transactionInfo.getDatabaseName() );

        FabricTransactionImpl fabricTransaction = new FabricTransactionImpl( transactionInfo,
                transactionBookmarkManager,
                remoteExecutor,
                localExecutor,
                logService,
                this,
                jobScheduler,
                fabricConfig );
        openTransactions.add( fabricTransaction );
        return fabricTransaction;
    }

    @Override
    public void stop()
    {
        openTransactions.forEach( FabricTransactionImpl::doRollback );
    }

    void removeTransaction( FabricTransactionImpl transaction )
    {
        openTransactions.remove( transaction );
    }

    public Set<FabricTransactionImpl> getOpenTransactions()
    {
        return openTransactions;
    }
}
