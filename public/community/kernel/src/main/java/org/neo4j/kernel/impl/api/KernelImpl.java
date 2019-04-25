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
package org.neo4j.kernel.impl.api;

import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.newapi.DefaultThreadSafeCursors;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.monitoring.Health;
import org.neo4j.storageengine.api.StorageEngine;

import static org.neo4j.configuration.GraphDatabaseSettings.transaction_timeout;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;

/**
 * This is the Neo4j Kernel, an implementation of the Kernel API which is an internal component used by Cypher and the
 * Core API (the API under org.neo4j.graphdb).
 *
 * <h1>Structure</h1>
 *
 * The Kernel lets you start transactions. The transactions allow you to create "statements", which, in turn, operate
 * against the database. Statements and transactions are separate concepts due to isolation requirements. A single
 * cypher query will normally use one statement, and there can be multiple statements executed in one transaction.
 *
 * Please refer to the {@link KernelTransaction} javadoc for details.
 *
 */
public class KernelImpl extends LifecycleAdapter implements InwardKernel
{
    private final KernelTransactions transactions;
    private final Health health;
    private final TransactionMonitor transactionMonitor;
    private final GlobalProcedures globalProcedures;
    private final Config config;
    private final DefaultThreadSafeCursors cursors;
    private volatile boolean isRunning;

    public KernelImpl( KernelTransactions transactionFactory, Health health,
                       TransactionMonitor transactionMonitor,
                       GlobalProcedures globalProcedures, Config config, StorageEngine storageEngine )
    {
        this.transactions = transactionFactory;
        this.health = health;
        this.transactionMonitor = transactionMonitor;
        this.globalProcedures = globalProcedures;
        this.config = config;
        this.cursors = new DefaultThreadSafeCursors( storageEngine.newReader() );
    }

    @Override
    public KernelTransaction beginTransaction( Transaction.Type type, LoginContext loginContext ) throws TransactionFailureException
    {
        return beginTransaction( type, loginContext, EMBEDDED_CONNECTION );
    }

    @Override
    public KernelTransaction beginTransaction( Transaction.Type type, LoginContext loginContext, ClientConnectionInfo connectionInfo )
            throws TransactionFailureException
    {
        if ( !isRunning )
        {
            throw new IllegalStateException( "Kernel is not running, so it is not possible to use it" );
        }
        return beginTransaction( type, loginContext, connectionInfo, config.get( transaction_timeout ).toMillis() );
    }

    @Override
    public KernelTransaction beginTransaction( Transaction.Type type, LoginContext loginContext, ClientConnectionInfo connectionInfo, long timeout ) throws
            TransactionFailureException
    {
        health.assertHealthy( TransactionFailureException.class );
        KernelTransaction transaction = transactions.newInstance( type, loginContext, connectionInfo, timeout );
        transactionMonitor.transactionStarted();
        return transaction;
    }

    @Override
    public void registerProcedure( CallableProcedure procedure ) throws ProcedureException
    {
        globalProcedures.register( procedure );
    }

    @Override
    public void registerUserFunction( CallableUserFunction function ) throws ProcedureException
    {
        globalProcedures.register( function );
    }

    @Override
    public void registerUserAggregationFunction( CallableUserAggregationFunction function ) throws ProcedureException
    {
        globalProcedures.register( function );
    }

    @Override
    public void start()
    {
        isRunning = true;
    }

    @Override
    public void stop()
    {
        if ( !isRunning )
        {
            throw new IllegalStateException( "Kernel is not running, so it is not possible to stop it" );
        }

        isRunning = false;
    }

    @Override
    public CursorFactory cursors()
    {
        return cursors;
    }

    @Override
    public void shutdown()
    {
        cursors.close();
    }
}
