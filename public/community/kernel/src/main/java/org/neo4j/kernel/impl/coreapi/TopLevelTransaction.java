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
package org.neo4j.kernel.impl.coreapi;

import java.util.Map;
import java.util.Optional;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.TransientFailureException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.Status.Classification;
import org.neo4j.kernel.api.exceptions.Status.Code;
import org.neo4j.kernel.impl.api.TokenAccess;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.traversal.BidirectionalTraversalDescriptionImpl;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.Terminated;

public class TopLevelTransaction implements InternalTransaction
{
    private static final PropertyContainerLocker locker = new PropertyContainerLocker();
    private boolean commitCalled;
    private final GraphDatabaseFacade facade;
    private KernelTransaction transaction;
    private final ThreadLocal<TopLevelTransaction> tempTopLevelTransaction;

    public TopLevelTransaction( GraphDatabaseFacade facade, KernelTransaction transaction, ThreadLocal<TopLevelTransaction> tempTopLevelTransaction )
    {
        this.facade = facade;
        this.transaction = transaction;
        this.tempTopLevelTransaction = tempTopLevelTransaction;
    }

    @Override
    public void commit()
    {
        safeTransactionOperation( Transaction::commit );
    }

    @Override
    public void rollback()
    {
        safeTransactionOperation( Transaction::rollback );
    }

    @Override
    public Node createNode()
    {
        try ( Statement ignore = transaction.acquireStatement() )
        {
            return facade.newNodeProxy( transaction.dataWrite().nodeCreate() );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Node createNode( Label... labels )
    {
        try ( Statement ignore = transaction.acquireStatement() )
        {
            TokenWrite tokenWrite = transaction.tokenWrite();
            int[] labelIds = new int[labels.length];
            String[] labelNames = new String[labels.length];
            for ( int i = 0; i < labelNames.length; i++ )
            {
                labelNames[i] = labels[i].name();
            }
            tokenWrite.labelGetOrCreateForNames( labelNames, labelIds );

            Write write = transaction.dataWrite();
            long nodeId = write.nodeCreateWithLabels( labelIds );
            return facade.newNodeProxy( nodeId );
        }
        catch ( ConstraintValidationException e )
        {
            throw new ConstraintViolationException( "Unable to add label.", e );
        }
        catch ( SchemaKernelException e )
        {
            throw new IllegalArgumentException( e );
        }
        catch ( KernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public BidirectionalTraversalDescription bidirectionalTraversalDescription()
    {
        return new BidirectionalTraversalDescriptionImpl( () -> ((KernelTransaction) kernelTransaction()).acquireStatement() );
    }

    @Override
    public TraversalDescription traversalDescription()
    {
        return new MonoDirectionalTraversalDescription( () -> ((KernelTransaction) kernelTransaction()).acquireStatement() );
    }

    @Override
    public ResourceIterable<Label> getAllLabelsInUse()
    {
        return allInUse( TokenAccess.LABELS );
    }

    @Override
    public ResourceIterable<RelationshipType> getAllRelationshipTypesInUse()
    {
        return allInUse( TokenAccess.RELATIONSHIP_TYPES );
    }

    @Override
    public ResourceIterable<Label> getAllLabels()
    {
        return all( TokenAccess.LABELS );
    }

    @Override
    public final void terminate()
    {
        transaction.markForTermination( Terminated );
    }

    @Override
    public void close()
    {
        tempTopLevelTransaction.remove();
        safeTransactionOperation( Transaction::close );
    }

    private void safeTransactionOperation( TransactionalOperation operation )
    {
        try
        {
            operation.perform( transaction );
        }
        catch ( TransientFailureException e )
        {
            // We let transient exceptions pass through unchanged since they aren't really transaction failures
            // in the same sense as unexpected failures are. Such exception signals that the transaction
            // can be retried and might be successful the next time.
            throw e;
        }
        catch ( ConstraintViolationTransactionFailureException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( KernelException | TransactionTerminatedException e )
        {
            Code statusCode = e.status().code();
            if ( statusCode.classification() == Classification.TransientError )
            {
                throw new TransientTransactionFailureException(
                        closeFailureMessage() + ": " + statusCode.description(), e );
            }
            throw new TransactionFailureException( closeFailureMessage(), e );
        }
        catch ( Exception e )
        {
            throw new TransactionFailureException( closeFailureMessage(), e );
        }
    }

    @Override
    public void setTransaction( KernelTransaction transaction )
    {
        this.transaction = transaction;
    }

    @FunctionalInterface
    private interface TransactionalOperation
    {
        void perform( KernelTransaction transaction ) throws Exception;
    }

    private String closeFailureMessage()
    {
        return commitCalled ? "Transaction failed to commit and was rolled back." : "Unable to rollback transaction";
    }

    @Override
    public Lock acquireWriteLock( PropertyContainer entity )
    {
        return locker.exclusiveLock( transaction, entity );
    }

    @Override
    public Lock acquireReadLock( PropertyContainer entity )
    {
        return locker.sharedLock(transaction, entity);
    }

    @Override
    public Transaction kernelTransaction()
    {
        return transaction;
    }

    @Override
    public KernelTransaction.Type transactionType()
    {
        return transaction.transactionType();
    }

    @Override
    public SecurityContext securityContext()
    {
        return transaction.securityContext();
    }

    @Override
    public ClientConnectionInfo clientInfo()
    {
        return transaction.clientInfo();
    }

    @Override
    public KernelTransaction.Revertable overrideWith( SecurityContext context )
    {
        return transaction.overrideWith( context );
    }

    @Override
    public Optional<Status> terminationReason()
    {
        return transaction.getReasonIfTerminated();
    }

    @Override
    public void setMetaData( Map<String,Object> txMeta )
    {
        transaction.setMetaData( txMeta );
    }

    private <T> ResourceIterable<T> allInUse( final TokenAccess<T> tokens )
    {
        return () -> tokens.inUse( (KernelTransaction) kernelTransaction() );
    }

    private <T> ResourceIterable<T> all( final TokenAccess<T> tokens )
    {
        return () -> tokens.all( (KernelTransaction) kernelTransaction() );
    }
}
