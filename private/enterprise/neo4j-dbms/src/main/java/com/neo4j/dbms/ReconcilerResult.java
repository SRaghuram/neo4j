/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.neo4j.kernel.database.NamedDatabaseId;

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
    private final CompletableFuture<Void> combinedFuture;

    ReconcilerResult( Map<String,CompletableFuture<ReconcilerStepResult>> reconciliationFutures )
    {
        this.reconciliationFutures = reconciliationFutures;
        this.combinedFuture = combineFutures( reconciliationFutures );
    }

    public void await( NamedDatabaseId namedDatabaseId )
    {
        var future = reconciliationFutures.get( namedDatabaseId.name() );
        if ( future != null )
        {
            future.join();
        }
    }

    public void await( Collection<NamedDatabaseId> namedDatabaseIds )
    {
        var futures = namedDatabaseIds.stream()
                .map( id -> reconciliationFutures.get( id.name() ) )
                .flatMap( Stream::ofNullable )
                .toArray( CompletableFuture<?>[]::new );

        CompletableFuture.allOf( futures ).join();
    }

    void awaitAll()
    {
        combinedFuture.join();
    }

    void whenComplete( Runnable action )
    {
        combinedFuture.whenComplete( ( ignore, error ) -> action.run() );
    }

    private static CompletableFuture<Void> combineFutures( Map<String,CompletableFuture<ReconcilerStepResult>> reconciliationFutures )
    {
        var allFutures = reconciliationFutures.values().toArray( CompletableFuture<?>[]::new );
        return CompletableFuture.allOf( allFutures );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        ReconcilerResult that = (ReconcilerResult) o;
        return Objects.equals( reconciliationFutures, that.reconciliationFutures );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( reconciliationFutures );
    }

    @Override
    public String toString()
    {
        return "ReconcilerResult{" + "reconciliationFutures=" + reconciliationFutures + '}';
    }
}
