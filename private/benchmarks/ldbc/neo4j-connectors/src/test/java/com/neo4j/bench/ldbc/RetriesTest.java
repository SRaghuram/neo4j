/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationInstances;
import org.junit.jupiter.api.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RetriesTest
{
    @Test
    public void shouldThrowExceptionAfterExcessiveDeadlockRetries() throws DbException
    {
        Retries retries = new Retries( DummyLdbcSnbInteractiveOperationInstances.read1() );
        assertThat( retries.allRetries(), equalTo( 0 ) );
        assertThat( retries.deadlockRetries(), equalTo( 0 ) );
        assertThat( retries.isolationRetries(), equalTo( 0 ) );
        assertThat( retries.retryableRetries(), equalTo( 0 ) );
        Exception e = new RuntimeException( "TOKEN EXCEPTION" );

        for ( int i = 0; i < Retries.maxDeadlockErrorCount(); i++ )
        {
            retries.incrementDeadlockDetectedCount( e );
        }

        assertThat( retries.deadlockRetries(), equalTo( Retries.maxDeadlockErrorCount() ) );
        assertThat( retries.isolationRetries(), equalTo( 0 ) );
        assertThat( retries.retryableRetries(), equalTo( 0 ) );

        boolean dbExceptionThrown = false;
        try
        {
            retries.incrementDeadlockDetectedCount( e );
        }
        catch ( DbException dbException )
        {
            dbException.printStackTrace();
            dbExceptionThrown = true;
        }

        assertTrue( dbExceptionThrown );
    }

    @Test
    public void shouldThrowExceptionAfterExcessiveIsolationRetries() throws DbException
    {
        Retries retries = new Retries( DummyLdbcSnbInteractiveOperationInstances.read1() );
        assertThat( retries.allRetries(), equalTo( 0 ) );
        assertThat( retries.deadlockRetries(), equalTo( 0 ) );
        assertThat( retries.isolationRetries(), equalTo( 0 ) );
        assertThat( retries.retryableRetries(), equalTo( 0 ) );
        Exception e = new RuntimeException( "TOKEN EXCEPTION" );

        for ( int i = 0; i < Retries.maxIsolationErrorCount() - 1; i++ )
        {
            retries.incrementNotFoundCount( e );
        }
        retries.incrementEntityNotFoundCount( e );

        assertThat( retries.deadlockRetries(), equalTo( 0 ) );
        assertThat( retries.isolationRetries(), equalTo( Retries.maxIsolationErrorCount() ) );
        assertThat( retries.retryableRetries(), equalTo( 0 ) );

        boolean dbExceptionThrown = false;
        try
        {
            retries.incrementNoSuchElementCount( e );
        }
        catch ( DbException dbException )
        {
            dbException.printStackTrace();
            dbExceptionThrown = true;
        }

        assertTrue( dbExceptionThrown );
    }

    @Test
    public void shouldThrowExceptionAfterExcessiveRetryableRetries() throws DbException
    {
        Retries retries = new Retries( DummyLdbcSnbInteractiveOperationInstances.read1() );
        assertThat( retries.allRetries(), equalTo( 0 ) );
        assertThat( retries.deadlockRetries(), equalTo( 0 ) );
        assertThat( retries.isolationRetries(), equalTo( 0 ) );
        assertThat( retries.retryableRetries(), equalTo( 0 ) );
        Exception e = new RuntimeException( "TOKEN EXCEPTION" );

        for ( int i = 0; i < Retries.maxRetryableErrorCount(); i++ )
        {
            retries.incrementRetryableErrorCount( e );
        }

        assertThat( retries.deadlockRetries(), equalTo( 0 ) );
        assertThat( retries.isolationRetries(), equalTo( 0 ) );
        assertThat( retries.retryableRetries(), equalTo( Retries.maxRetryableErrorCount() ) );

        boolean dbExceptionThrown = false;
        try
        {
            retries.incrementRetryableErrorCount( e );
        }
        catch ( DbException dbException )
        {
            dbException.printStackTrace();
            dbExceptionThrown = true;
        }

        assertTrue( dbExceptionThrown );
    }

    @Test
    public void shouldEncodeAndDecodeResultCode1()
    {
        Retries retries = new Retries( DummyLdbcSnbInteractiveOperationInstances.read1() );
        assertThat( retries.allRetries(), equalTo( 0 ) );
        assertThat( retries.deadlockRetries(), equalTo( 0 ) );
        assertThat( retries.isolationRetries(), equalTo( 0 ) );
        assertThat( retries.retryableRetries(), equalTo( 0 ) );

        doEncodeDecodeTest( retries );
    }

    @Test
    public void shouldEncodeAndDecodeResultCode2() throws DbException
    {
        Retries retries = new Retries( DummyLdbcSnbInteractiveOperationInstances.read1() );
        assertThat( retries.allRetries(), equalTo( 0 ) );
        assertThat( retries.deadlockRetries(), equalTo( 0 ) );
        assertThat( retries.isolationRetries(), equalTo( 0 ) );
        assertThat( retries.retryableRetries(), equalTo( 0 ) );
        Exception e = new RuntimeException( "TOKEN EXCEPTION" );

        retries.incrementEntityNotFoundCount( e );
        retries.incrementNotFoundCount( e );
        retries.incrementNoSuchElementCount( e );
        retries.incrementDeadlockDetectedCount( e );
        retries.incrementRetryableErrorCount( e );

        assertThat( retries.deadlockRetries(), equalTo( 1 ) );
        assertThat( retries.isolationRetries(), equalTo( 3 ) );
        assertThat( retries.retryableRetries(), equalTo( 1 ) );

        doEncodeDecodeTest( retries );
    }

    @Test
    public void shouldEncodeAndDecodeResultCode3() throws DbException
    {
        Retries retries = new Retries( DummyLdbcSnbInteractiveOperationInstances.read1() );
        assertThat( retries.allRetries(), equalTo( 0 ) );
        assertThat( retries.deadlockRetries(), equalTo( 0 ) );
        assertThat( retries.isolationRetries(), equalTo( 0 ) );
        assertThat( retries.retryableRetries(), equalTo( 0 ) );
        Exception e = new RuntimeException( "TOKEN EXCEPTION" );

        for ( int i = 0; i < Retries.maxDeadlockErrorCount(); i++ )
        {
            retries.incrementDeadlockDetectedCount( e );
        }
        retries.incrementEntityNotFoundCount( e );
        retries.incrementNotFoundCount( e );
        retries.incrementNoSuchElementCount( e );
        retries.incrementRetryableErrorCount( e );

        assertThat( retries.deadlockRetries(), equalTo( Retries.maxDeadlockErrorCount() ) );
        assertThat( retries.isolationRetries(), equalTo( 3 ) );
        assertThat( retries.retryableRetries(), equalTo( 1 ) );

        doEncodeDecodeTest( retries );
    }

    @Test
    public void shouldEncodeAndDecodeResultCode4() throws DbException
    {
        Retries retries = new Retries( DummyLdbcSnbInteractiveOperationInstances.read1() );
        assertThat( retries.allRetries(), equalTo( 0 ) );
        assertThat( retries.deadlockRetries(), equalTo( 0 ) );
        assertThat( retries.isolationRetries(), equalTo( 0 ) );
        assertThat( retries.retryableRetries(), equalTo( 0 ) );
        Exception e = new RuntimeException( "TOKEN EXCEPTION" );

        for ( int i = 0; i < Retries.maxDeadlockErrorCount(); i++ )
        {
            retries.incrementDeadlockDetectedCount( e );
        }

        for ( int i = 0; i < Retries.maxIsolationErrorCount(); i++ )
        {
            retries.incrementEntityNotFoundCount( e );
        }

        for ( int i = 0; i < Retries.maxRetryableErrorCount(); i++ )
        {
            retries.incrementRetryableErrorCount( e );
        }

        assertThat( retries.deadlockRetries(), equalTo( Retries.maxDeadlockErrorCount() ) );
        assertThat( retries.isolationRetries(), equalTo( Retries.maxIsolationErrorCount() ) );
        assertThat( retries.retryableRetries(), equalTo( Retries.maxRetryableErrorCount() ) );

        doEncodeDecodeTest( retries );
    }

    private void doEncodeDecodeTest( Retries retries )
    {
        assertThat( retries.allRetries(),
                equalTo(
                        retries.deadlockRetries() +
                        retries.isolationRetries() +
                        retries.retryableRetries()
                ) );

        int resultCode = Retries.encodeRetriesToResultCode( retries );

        byte decodedIsolationRetryCount = Retries.decodeResultCodeToIsolationRetryCount( resultCode );
        byte decodedDeadlockRetryCount = Retries.decodeResultCodeToDeadlockRetryCount( resultCode );
        byte decodedRetryableRetryCount = Retries.decodeResultCodeToRetryableRetryCount( resultCode );

        assertThat( (int) decodedDeadlockRetryCount, equalTo( retries.deadlockRetries() ) );
        assertThat( (int) decodedIsolationRetryCount, equalTo( retries.isolationRetries() ) );
        assertThat( (int) decodedRetryableRetryCount, equalTo( retries.retryableRetries() ) );
    }
}
