/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

import java.util.concurrent.CompletableFuture;

class CloseSignalFailureHandler implements FailureEventHandler
{
    private final CompletableFuture<?> completableFuture;

    CloseSignalFailureHandler( CompletableFuture<?> completableFuture )
    {
        this.completableFuture = completableFuture;
    }

    @Override
    public void onFailure( Exception e )
    {
        completableFuture.completeExceptionally( e );
    }
}
