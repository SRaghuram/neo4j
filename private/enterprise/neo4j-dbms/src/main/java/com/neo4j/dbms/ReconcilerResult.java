/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.neo4j.kernel.database.DatabaseId;

/**
 * Simple holder for a Collection of concurrently executing {@link CompletableFuture}s
 * returned from {@link DbmsReconciler#reconcile(List, ReconcilerRequest)}. Provides the ability
 * for calling methods to selectively block on reconciler completion.
 *
 * Note: awaiting on the reconciliation of particular databases is relaxed. If you await
 * databaseIds which are unrecognised the method will simply return immediately rather than
 * throw any kind of exception.
 */
public final class ReconcilerResult
{
    public static final ReconcilerResult EMPTY = new ReconcilerResult( Collections.emptyMap() );

    private final Map<String,CompletableFuture<ReconcilerStepResult>> reconciliationFutures;
    private final CompletableFuture<Void> completedFuture;

    ReconcilerResult( Map<String,CompletableFuture<ReconcilerStepResult>> reconciliationFutures )
    {
        this.reconciliationFutures = reconciliationFutures;
        this.completedFuture = buildCompletedFuture( reconciliationFutures );
    }

    public void await( DatabaseId databaseId )
    {
        var future = reconciliationFutures.get( databaseId.name() );
        if ( future != null )
        {
            future.join();
        }
    }

    public void await( Collection<DatabaseId> databaseIds )
    {
        var futures = databaseIds.stream()
                .map( id -> reconciliationFutures.get( id.name() ) )
                .flatMap( Stream::ofNullable )
                .toArray( CompletableFuture<?>[]::new );

        CompletableFuture.allOf( futures ).join();
    }

    void awaitAll()
    {
        completedFuture.join();
    }

    void whenComplete( Runnable action )
    {
        completedFuture.whenComplete( ( ignore, error ) -> action.run() );
    }

    private static CompletableFuture<Void> buildCompletedFuture( Map<String,CompletableFuture<ReconcilerStepResult>> reconciliationFutures )
    {
        var allFutures = reconciliationFutures.values().toArray( CompletableFuture<?>[]::new );
        return CompletableFuture.allOf( allFutures );
    }
}
