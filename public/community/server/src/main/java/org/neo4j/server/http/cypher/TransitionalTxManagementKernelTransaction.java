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
package org.neo4j.server.http.cypher;

import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public class TransitionalTxManagementKernelTransaction
{
    private final GraphDatabaseFacade db;
    private final KernelTransaction.Type type;
    private final LoginContext loginContext;
    private final long customTransactionTimeout;
    private final ClientConnectionInfo connectionInfo;

    private InternalTransaction tx;
    private KernelTransaction suspendedTransaction;

    public TransitionalTxManagementKernelTransaction( GraphDatabaseFacade db, KernelTransaction.Type type, LoginContext loginContext,
            ClientConnectionInfo connectionInfo, long customTransactionTimeout )
    {
        this.db = db;
        this.type = type;
        this.loginContext = loginContext;
        this.customTransactionTimeout = customTransactionTimeout;
        this.connectionInfo = connectionInfo;
        this.tx = startTransaction();
    }

    public InternalTransaction getInternalTransaction()
    {
        return tx;
    }

    void suspendSinceTransactionsAreStillThreadBound()
    {
        assert suspendedTransaction == null : "Can't suspend the transaction if it already is suspended.";
//        suspendedTransaction = bridge.getKernelTransactionBoundToThisThread( true, db.databaseId() );
//        bridge.unbindTransactionFromCurrentThread();
    }

    void resumeSinceTransactionsAreStillThreadBound()
    {
        assert suspendedTransaction != null : "Can't resume the transaction if it has not first been suspended.";
//        bridge.bindTransactionToCurrentThread( suspendedTransaction );
//        suspendedTransaction = null;
    }

    public void terminate()
    {
        tx.terminate();
    }

    public void rollback()
    {
//        try
//        {
//            KernelTransaction kernelTransactionBoundToThisThread = bridge.getKernelTransactionBoundToThisThread( false, db.databaseId() );
//            if ( kernelTransactionBoundToThisThread != null )
//            {
//                kernelTransactionBoundToThisThread.rollback();
//            }
//        }
//        catch ( TransactionFailureException e )
//        {
//            throw new RuntimeException( e );
//        }
//        finally
//        {
//            bridge.unbindTransactionFromCurrentThread();
//        }
    }

    public void commit()
    {
//        try
//        {
//            KernelTransaction kernelTransactionBoundToThisThread = bridge.getKernelTransactionBoundToThisThread( true, db.databaseId() );
//            kernelTransactionBoundToThisThread.commit();
//        }
//        catch ( NotInTransactionException e )
//        {
//            // if the transaction was already terminated there is nothing more to do
//        }
//        catch ( TransactionFailureException e )
//        {
//            throw new RuntimeException( e );
//        }
//        finally
//        {
//            bridge.unbindTransactionFromCurrentThread();
//        }
    }

    void closeTransactionForPeriodicCommit()
    {
        tx.close();
    }

    public void reopenAfterPeriodicCommit()
    {
        tx = startTransaction();
    }

    private InternalTransaction startTransaction()
    {
        return customTransactionTimeout > GraphDatabaseSettings.UNSPECIFIED_TIMEOUT ? db.beginTransaction( type, loginContext, connectionInfo,
                customTransactionTimeout, TimeUnit.MILLISECONDS ) : db.beginTransaction( type, loginContext, connectionInfo );
    }
}
