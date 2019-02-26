/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class NettyUtil
{
    public static <T> CompletableFuture<T> toCompletableFuture( Future<T> nettyFuture )
    {
        return toCompletableFuture( nettyFuture, Function.identity() );
    }

    public static <T, R> CompletableFuture<R> toCompletableFuture( Future<T> nettyFuture, Function<T,R> conversion )
    {
        CompletableFuture<R> javaFuture = new CompletableFuture<>();
        nettyFuture.addListener( (GenericFutureListener<Future<T>>) f ->
        {
            if ( f.isSuccess() )
            {
                javaFuture.complete( conversion.apply( f.get() ) );
            }
            else
            {
                javaFuture.completeExceptionally( f.cause() );
            }
        } );
        return javaFuture;
    }
}
