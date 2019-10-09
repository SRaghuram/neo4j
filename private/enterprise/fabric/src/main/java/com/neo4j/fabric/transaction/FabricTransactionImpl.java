/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.transaction;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.executor.FabricException;
import com.neo4j.fabric.executor.FabricLocalExecutor;
import com.neo4j.fabric.executor.FabricRemoteExecutor;
import com.neo4j.fabric.stream.StatementResult;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.scheduler.Group.SERVER_TRANSACTION_TIMEOUT;

public class FabricTransactionImpl implements FabricTransaction, FabricTransaction.FabricExecutionContext
{
    private static final AtomicLong ID_GENERATOR = new AtomicLong();

    private final FabricRemoteExecutor remoteExecutor;
    private final FabricLocalExecutor localExecutor;
    private final Log userLog;
    private final Log internalLog;
    private final TransactionManager transactionManager;
    private final JobScheduler jobScheduler;
    private final FabricConfig fabricConfig;
    private final long id;
    private JobHandle timeoutHandle;
    private FabricRemoteExecutor.FabricRemoteTransaction remoteTransaction;
    private FabricLocalExecutor.FabricLocalTransaction localTransaction;
    private boolean terminated;
    private Status terminationStatus;

    FabricTransactionImpl( FabricRemoteExecutor remoteExecutor, FabricLocalExecutor localExecutor, LogService logService,
            TransactionManager transactionManager, JobScheduler jobScheduler,
            FabricConfig fabricConfig )
    {
        this.remoteExecutor = remoteExecutor;
        this.localExecutor = localExecutor;
        this.userLog = logService.getUserLog( FabricTransactionImpl.class );
        this.internalLog = logService.getInternalLog( FabricTransactionImpl.class );
        this.transactionManager = transactionManager;
        this.jobScheduler = jobScheduler;
        this.fabricConfig = fabricConfig;
        this.id = ID_GENERATOR.incrementAndGet();
    }

    @Override
    public FabricRemoteExecutor.FabricRemoteTransaction getRemote()
    {
        return remoteTransaction;
    }

    @Override
    public FabricLocalExecutor.FabricLocalTransaction getLocal()
    {
        return localTransaction;
    }

    public void begin( FabricTransactionInfo transactionInfo )
    {
        internalLog.debug( "Starting transaction %d", id );

        try
        {
            remoteTransaction = remoteExecutor.begin( transactionInfo );
            localTransaction = localExecutor.begin( transactionInfo );
            scheduleTimeout( transactionInfo );
            internalLog.debug( "Transaction %d started", id );
        }
        catch ( RuntimeException e )
        {
            // the exception with stack trace will be logged by Bolt's ErrorReporter
            userLog.error( "Transaction {} start failed", id );
            throw transform( Status.Transaction.TransactionStartFailed, e );
        }
    }

    public StatementResult execute( Function<FabricExecutionContext,StatementResult> runLogic )
    {
        if ( terminated )
        {
            Status status = terminationStatus;
            if ( status == null )
            {
                status = Status.Statement.ExecutionFailed;
            }

            internalLog.error( "Trying to execute query in a terminated transaction %d", id );
            throw new FabricException( status, "Trying to execute query in a terminated transaction" );
        }

        try
        {
            return runLogic.apply( this );
        }
        catch ( RuntimeException e )
        {
            // the exception with stack trace will be logged by Bolt's ErrorReporter
            userLog.error( "Query execution in transaction %d failed", id );
            doRollback();
            throw transform( Status.Statement.ExecutionFailed, e );
        }
    }

    public void commit()
    {
        // the transaction has failed and been rolled back as part of the failure clean up
        if ( terminated )
        {
            throw new FabricException( Status.Transaction.TransactionCommitFailed, "Trying to commit terminated transaction" );
        }
        terminated = true;

        internalLog.debug( "Committing transaction %d", id );
        cancelTimeout();

        try
        {
            if ( localTransaction != null )
            {
                localTransaction.commit();
            }
        }
        catch ( Exception e )
        {
            // failure to commit a local transaction is just logged and not reported to the user
            userLog.error( String.format( "Local transaction %d commit failed", id ), e );
        }

        try
        {
            if ( remoteTransaction != null )
            {
                remoteTransaction.commit().block();
            }
        }
        catch ( Exception e )
        {
            // the exception with stack trace will be logged by Bolt's ErrorReporter
            userLog.error( "Transaction %d commit failed", id );
            throw new FabricException( Status.Transaction.TransactionCommitFailed, "Failed to commit remote transaction", e );
        }
        finally
        {
            transactionManager.removeTransaction( this );
        }

        internalLog.debug( "Transaction %d committed", id );
    }

    public void rollback()
    {
        // guard against someone calling rollback after 'begin' failure
        if ( remoteTransaction == null && localTransaction == null )
        {
            return;
        }

        doRollback();
    }

    @Override
    public void markForTermination( Status reason )
    {
        internalLog.debug( "Terminating transaction %d", id );
        terminationStatus = reason;

        localTransaction.markForTermination( reason );
        doRollback();
    }

    @Override
    public Optional<Status> getReasonIfTerminated()
    {
        return Optional.empty();
    }

    void doRollback()
    {
        // the transaction has already been rolled back as part of the failure clean up
        if ( terminated )
        {
            return;
        }

        terminated = true;
        internalLog.debug( "Rolling back transaction %d", id );
        cancelTimeout();

        try
        {
            if ( localTransaction != null )
            {
                localTransaction.rollback();
            }
        }
        catch ( Exception e )
        {
            // failure to rollback a local transaction is just logged and not reported to the user
            userLog.error( String.format( "Local transaction %d rollback failed", id ), e );
        }

        try
        {
            if ( remoteTransaction != null )
            {
                remoteTransaction.rollback().block();
            }
        }
        catch ( Exception e )
        {
            // the exception with stack trace will be logged by Bolt's ErrorReporter
            userLog.error( "Transaction %d rollback failed", id );
            throw new FabricException( Status.Transaction.TransactionRollbackFailed, "Failed to rollback remote transaction", e );
        }
        finally
        {
            transactionManager.removeTransaction( this );
        }

        internalLog.debug( "Transaction %d rolled back", id );
    }

    private RuntimeException transform( Status defaultStatus, Throwable t )
    {
        if ( t instanceof Status.HasStatus )
        {
            return new FabricException( ((Status.HasStatus) t).status(), t );
        }

        return new FabricException( defaultStatus, t );
    }

    private void scheduleTimeout( FabricTransactionInfo transactionInfo )
    {
        if ( transactionInfo.getTxTimeout() != null )
        {
            scheduleTimeout( transactionInfo.getTxTimeout() );
            return;
        }

        scheduleTimeout( fabricConfig.getTransactionTimeout() );
    }

    private void scheduleTimeout( Duration duration )
    {
        // 0 means no timeout
        if ( duration.equals( Duration.ZERO ) )
        {
            return;
        }

        timeoutHandle = jobScheduler.schedule( SERVER_TRANSACTION_TIMEOUT, this::handleTimeout, duration.toSeconds(), TimeUnit.SECONDS );
    }

    private void handleTimeout()
    {
        // the transaction has already been rolled back as part of the failure clean up
        if ( terminated )
        {
            return;
        }

        userLog.info( "Terminating transaction %d because of timeout", id );
        terminationStatus = Status.Transaction.TransactionTimedOut;
        doRollback();
    }

    private void cancelTimeout()
    {
        if ( timeoutHandle != null )
        {
            timeoutHandle.cancel();
        }
    }
}
