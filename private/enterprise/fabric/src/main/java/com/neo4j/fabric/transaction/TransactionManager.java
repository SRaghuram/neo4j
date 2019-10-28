/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.transaction;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.executor.Exceptions;
import com.neo4j.fabric.executor.FabricLocalExecutor;
import com.neo4j.fabric.executor.FabricRemoteExecutor;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.common.DependencyResolver;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.exceptions.Status;
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

    private final Set<FabricTransactionImpl> openTransactions = new HashSet<>();

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
        authorize( transactionInfo.getLoginContext(), transactionInfo.getDatabaseName() );

        FabricTransactionImpl fabricTransaction = new FabricTransactionImpl( transactionInfo,
                transactionBookmarkManager,
                remoteExecutor,
                localExecutor,
                logService,
                this,
                jobScheduler,
                fabricConfig );
        fabricTransaction.begin();
        openTransactions.add( fabricTransaction );
        return fabricTransaction;
    }

    private void authorize( LoginContext loginContext, String dbName )
    {
        try
        {
            loginContext.authorize( LoginContext.IdLookup.EMPTY, dbName );
        }
        catch ( KernelException e )
        {
            throw Exceptions.transform( Status.Security.Forbidden, e );
        }
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
