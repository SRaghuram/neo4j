/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.neo4j.causalclustering.net.NettyUtil.chain;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NettyUtilTest
{
    private EventExecutor executor = GlobalEventExecutor.INSTANCE;

    @Test
    void shouldReturnPromiseFromChain()
    {
        Promise<Object> future = executor.newPromise();
        Promise<Object> promise = executor.newPromise();

        Future<Object> out = chain( future, promise );

        assertEquals( promise, out );
    }

    @Test
    void shouldFulfillChainedPromiseOnFutureSuccess() throws InterruptedException, ExecutionException, TimeoutException
    {
        Promise<Object> future = executor.newPromise();
        Promise<Object> promise = executor.newPromise();

        chain( future, promise );

        assertFalse( promise.isDone() );
        assertFalse( future.isDone() );

        Object result = new Object();
        future.setSuccess( result );

        assertEquals( result, promise.get( 1, TimeUnit.MINUTES ) );
    }

    @Test
    void shouldFulfillChainedPromiseOnFutureFailure() throws InterruptedException, ExecutionException, TimeoutException
    {
        Promise<Object> future = executor.newPromise();
        Promise<Object> promise = executor.newPromise();

        chain( future, promise );

        assertFalse( promise.isDone() );
        assertFalse( future.isDone() );

        Exception cause = new Exception();
        future.setFailure( cause );

        ExecutionException exception = assertThrows( ExecutionException.class, () -> promise.get( 1, TimeUnit.MINUTES ) );
        assertEquals( cause, exception.getCause() );
    }
}
