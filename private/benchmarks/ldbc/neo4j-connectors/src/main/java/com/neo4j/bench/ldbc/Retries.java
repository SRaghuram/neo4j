/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc;

import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.neo4j.exceptions.CypherExecutionException;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.api.exceptions.Status;

public class Retries<OPERATION_TYPE extends Operation<OPERATION_RESULT_TYPE>, OPERATION_RESULT_TYPE>
{
    private static final int DEADLOCK_ERROR_MAX = 63;
    private static final int ISOLATION_ERROR_MAX = 31;
    private static final int RETRYABLE_ERROR_MAX = 31;

    private final OPERATION_TYPE operation;
    private int noSuchElementCount;
    private int notFoundCount;
    private int deadlockDetectedCount;
    private int entityNotFoundCount;
    private int retryableErrorCount;
    private OPERATION_RESULT_TYPE result;
    private int resultCode;

    private enum ErrorReason
    {
        EXCESSIVE_ISOLATION_RELATED_RETRIES,
        EXCESSIVE_DEADLOCK_RELATED_RETRIES,
        EXCESSIVE_RETRYABLE_RELATED_RETRIES,
        LDBC_DRIVER_RELATED,
        UNEXPECTED_ERROR
    }

    Retries( OPERATION_TYPE operation )
    {
        this.operation = operation;
        this.noSuchElementCount = 0;
        this.notFoundCount = 0;
        this.deadlockDetectedCount = 0;
        this.entityNotFoundCount = 0;
        this.retryableErrorCount = 0;
        this.result = null;
    }

    /**
     * @return retry count as an integer, where high byte contains isolation-related retry count and low byte
     * contains deadlock related retries.
     * [     5    |    5     |    6    ]
     * [RETRYABLE | ISOLATION| DEADLOCK]
     */
    static int encodeRetriesToResultCode( Retries retries )
    {
        return (retries.retryableRetries() << 11) | (retries.isolationRetries() << 6) | retries.deadlockRetries();
    }

    public static byte decodeResultCodeToRetryableRetryCount( int resultCode )
    {
        return (byte) ((resultCode >> 11) & 0x1f);
    }

    public static byte decodeResultCodeToIsolationRetryCount( int resultCode )
    {
        return (byte) ((resultCode >> 6) & 0x1f);
    }

    public static byte decodeResultCodeToDeadlockRetryCount( int resultCode )
    {
        return (byte) (resultCode & 0x003f);
    }

    static int maxDeadlockErrorCount()
    {
        return DEADLOCK_ERROR_MAX;
    }

    static int maxIsolationErrorCount()
    {
        return ISOLATION_ERROR_MAX;
    }

    static int maxRetryableErrorCount()
    {
        return RETRYABLE_ERROR_MAX;
    }

    public int encodedResultsCode()
    {
        return resultCode;
    }

    public int deadlockRetries()
    {
        return deadlockDetectedCount;
    }

    public int isolationRetries()
    {
        return noSuchElementCount + notFoundCount + entityNotFoundCount;
    }

    public int retryableRetries()
    {
        return retryableErrorCount;
    }

    public int allRetries()
    {
        return deadlockRetries() + isolationRetries() + retryableRetries();
    }

    private void setResult( OPERATION_RESULT_TYPE result )
    {
        this.result = result;
    }

    public OPERATION_RESULT_TYPE result()
    {
        return result;
    }

    public static <INPUT extends Operation<OUTPUT>, OUTPUT> Retries<INPUT,OUTPUT> run(
            Neo4jQuery<INPUT,OUTPUT,Neo4jConnectionState> query,
            INPUT operation,
            Neo4jConnectionState dbConnectionState
    ) throws DbException
    {
        OUTPUT result = null;
        Retries<INPUT,OUTPUT> retries = new Retries<>( operation );
        boolean succeeded;
        do
        {
            succeeded = false;
            try
            {
                result = query.execute( dbConnectionState, operation );
                succeeded = true;
            }
            catch ( Exception e )
            {
                backOffOrFail( retries, e );
            }
        }
        while ( !succeeded );
        retries.setResult( result );
        return retries;
    }

    public static <INPUT extends Operation<OUTPUT>, OUTPUT> Retries<INPUT,OUTPUT> runInTransaction(
            Neo4jQuery<INPUT,OUTPUT,Neo4jConnectionState> query,
            INPUT operation,
            Neo4jConnectionState connection ) throws DbException
    {
        OUTPUT result = null;
        Retries<INPUT,OUTPUT> retries = new Retries<>( operation );
        boolean succeeded;
        do
        {
            succeeded = false;
            try ( Transaction tx = connection.beginTx() )
            {
                result = query.execute( connection, operation );
                tx.commit();
                succeeded = true;
            }
            catch ( Exception e )
            {
                backOffOrFail( retries, e );
            }
            finally
            {
                connection.freeTx();
            }
        }
        while ( !succeeded );
        retries.setResult( result );
        return retries;
    }

    void incrementNoSuchElementCount( Throwable e ) throws DbException
    {
        noSuchElementCount++;
        if ( isolationRetries() > ISOLATION_ERROR_MAX )
        {
            throw new DbException( errorMessage( this, ErrorReason.EXCESSIVE_ISOLATION_RELATED_RETRIES ), e );
        }
        resultCode = encodeRetriesToResultCode( this );
    }

    void incrementNotFoundCount( Throwable e ) throws DbException
    {
        notFoundCount++;
        if ( isolationRetries() > ISOLATION_ERROR_MAX )
        {
            throw new DbException( errorMessage( this, ErrorReason.EXCESSIVE_ISOLATION_RELATED_RETRIES ), e );
        }
        resultCode = encodeRetriesToResultCode( this );
    }

    void incrementEntityNotFoundCount( Throwable e ) throws DbException
    {
        entityNotFoundCount++;
        if ( isolationRetries() > ISOLATION_ERROR_MAX )
        {
            throw new DbException( errorMessage( this, ErrorReason.EXCESSIVE_ISOLATION_RELATED_RETRIES ), e );
        }
        resultCode = encodeRetriesToResultCode( this );
    }

    void incrementDeadlockDetectedCount( Throwable e ) throws DbException
    {
        deadlockDetectedCount++;
        if ( deadlockRetries() > DEADLOCK_ERROR_MAX )
        {
            throw new DbException( errorMessage( this, ErrorReason.EXCESSIVE_DEADLOCK_RELATED_RETRIES ), e );
        }
        resultCode = encodeRetriesToResultCode( this );
    }

    void incrementRetryableErrorCount( Throwable e ) throws DbException
    {
        retryableErrorCount++;
        if ( retryableRetries() > RETRYABLE_ERROR_MAX )
        {
            throw new DbException( errorMessage( this, ErrorReason.EXCESSIVE_RETRYABLE_RELATED_RETRIES ), e );
        }
        resultCode = encodeRetriesToResultCode( this );
    }

    private static void backOffOrFail( Retries retries, Throwable e ) throws DbException
    {
        if ( e instanceof NoSuchElementException )
        {
            // isolation bug, try again
            retries.incrementNoSuchElementCount( e );
            backOff( retries );
        }
        else if ( e instanceof NotFoundException )
        {
            // isolation bug, try again
            retries.incrementNotFoundCount( e );
            backOff( retries );
        }
        else if ( e instanceof DeadlockDetectedException )
        {
            retries.incrementDeadlockDetectedCount( e );
            backOff( retries );
        }
        else if ( e instanceof CypherExecutionException &&
                  ((CypherExecutionException) e).status().equals( Status.Statement.EntityNotFound ) )
        {
            // isolation bug, try again
            retries.incrementEntityNotFoundCount( e );
            backOff( retries );
        }
        else if ( e instanceof RetryableErrorException )
        {
            retries.incrementRetryableErrorCount( e );
            backOff( retries );
        }
        else if ( e instanceof DbException )
        {
            Throwable cause = e.getCause();
            if ( null == cause )
            {
                throw new DbException( errorMessage( retries, ErrorReason.LDBC_DRIVER_RELATED ), e );
            }
            else
            {
                backOffOrFail( retries, cause );
            }
        }
        else
        {
            throw new DbException( errorMessage( retries, ErrorReason.UNEXPECTED_ERROR ), e );
        }
    }

    private static String errorMessage( Retries retries, ErrorReason reason )
    {
        String indent = "  ";
        return "Error Executing: " + retries.operation + "\n" +
               "Reason: " + reason.name() + "\n" +
               "Retries:\n" +
               indent + NoSuchElementException.class.getSimpleName() + " = " + retries.noSuchElementCount + "\n" +
               indent + NotFoundException.class.getSimpleName() + " = " + retries.notFoundCount + "\n" +
               indent + CypherExecutionException.class.getSimpleName() + ":" +
               Status.Statement.EntityNotFound.name() + " = " + retries.entityNotFoundCount + "\n" +
               indent + DeadlockDetectedException.class.getSimpleName() + " = " + retries.deadlockDetectedCount + "\n" +
               indent + RetryableErrorException.class.getSimpleName() + " = " + retries.retryableErrorCount + "\n";
    }

    private static void backOff( Retries retries )
    {
        double random = new Random( System.currentTimeMillis() ).nextDouble();
        // long sleepMilli = Math.round( TimeUnit.SECONDS.toMillis( retries.deadlockDetectedCount ) * random );
        long sleepMilli = Math.round(
                TimeUnit.SECONDS.toMillis( Math.round( Math.pow( 2, retries.allRetries() ) ) ) * random
        );
        Spinner.powerNap( sleepMilli );
    }
}
