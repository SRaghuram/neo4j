/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.concurrent.CompletableFuture;

public class NettyUtil
{
    public static <T> CompletableFuture<T> toCompletableFuture( Future<T> nettyFuture )
    {
        CompletableFuture<T> javaFuture = new CompletableFuture<>();
        nettyFuture.addListener( (GenericFutureListener<Future<T>>) f ->
        {
            if ( f.isSuccess() )
            {
                javaFuture.complete( f.get() );
            }
            else
            {
                javaFuture.completeExceptionally( f.cause() );
            }
        } );
        return javaFuture;
    }
}
