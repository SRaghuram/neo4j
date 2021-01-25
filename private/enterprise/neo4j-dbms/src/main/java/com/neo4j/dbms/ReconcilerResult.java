/*
 * Copyright (c) "Neo4j"
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.helpers.collection.Pair;
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

    /**
     * Await the completion of reconciliation jobs for the given database
     * @throws DatabaseManagementException error which occurs during the reconciliation job, if any
     */
    public void join( NamedDatabaseId namedDatabaseId ) throws DatabaseManagementException
    {
        var future = reconciliationFutures.get( namedDatabaseId.name() );
        if ( future != null )
        {
            var result = future.join();
            if ( result.error() != null )
            {
                throw new DatabaseManagementException( "A triggered DbmsReconciler job failed with the following cause", result.error() );
            }
        }
    }

    /**
     * Await the completion of the reconciliation jobs for the given databases
     * @throws DatabaseManagementException exception suppressing/containing all errors which occur during any of the reconciliation jobs
     */
    public void join( Collection<NamedDatabaseId> namedDatabaseIds ) throws DatabaseManagementException
    {
        collectAndThrowErrors( getFutures( namedDatabaseIds ) );
    }

    /**
     * Await the completion of the reconciliation jobs for all databases
     * @throws DatabaseManagementException exception suppressing/containing all errors which occur during any of the reconciliation jobs
     */
    public void joinAll() throws DatabaseManagementException
    {
        collectAndThrowErrors( reconciliationFutures.values() );
    }

    private void collectAndThrowErrors( Collection<CompletableFuture<ReconcilerStepResult>> futures ) throws DatabaseManagementException
    {
        var errors = futures.stream()
                .map( CompletableFuture::join )
                .filter( result -> result.error() != null )
                .collect( Collectors.toMap( result -> result.desiredState().databaseId(), ReconcilerStepResult::error ) );

        if ( !errors.isEmpty() )
        {
            var databases = errors.keySet().stream()
                    .map( NamedDatabaseId::name )
                    .collect( Collectors.joining( ",", "[", "]" ) );

            var cause =  errors.values().stream().reduce( null, Exceptions::chain ); //Build a composite error
            throw new DatabaseManagementException( String.format( "Reconciliation failed for databases %s", databases ), cause );
        }
    }

    /**
     * Await the completion of the reconciliation job for the given database
     *
     * Note: despite the use of {@code future.join()} internally, this method should not throw. This is because all exceptions which occur
     * during reconciliation jobs are caught and wrapped in {@link ReconcilerStepResult}s. If you wish to rethrow any exceptions which may
     * have occurred, please use {@link ReconcilerResult#join(NamedDatabaseId)} or similar.
     */
    public void await( NamedDatabaseId namedDatabaseId )
    {
        var future = reconciliationFutures.get( namedDatabaseId.name() );
        if ( future != null )
        {
            future.join();
        }
    }

    /**
     * Await the completion of reconciliation jobs for the given databases
     *
     * Note: despite the use of {@code future.join()} internally, this method should not throw. This is because all exceptions which occur
     * during reconciliation jobs are caught and wrapped in {@link ReconcilerStepResult}s. If you wish to rethrow any exceptions which may
     * have occurred, please use {@link ReconcilerResult#join(NamedDatabaseId)} or similar.
     */
    public void await( Collection<NamedDatabaseId> namedDatabaseIds )
    {
        var futuresArray = getFutures( namedDatabaseIds ).toArray( CompletableFuture<?>[]::new );
        CompletableFuture.allOf( futuresArray ).join();
    }

    /**
     * Await the completion of reconciliation jobs for all databases
     *
     * Note: despite the use of {@code future.join()} internally, this method should not throw. This is because all exceptions which occur
     * during reconciliation jobs are caught and wrapped in {@link ReconcilerStepResult}s. If you wish to rethrow any exceptions which may
     * have occurred, please use {@link ReconcilerResult#join(NamedDatabaseId)} or similar.
     */
    void awaitAll()
    {
        combinedFuture.join();
    }

    void whenComplete( Runnable action )
    {
        combinedFuture.whenComplete( ( ignore, error ) -> action.run() );
    }

    private List<CompletableFuture<ReconcilerStepResult>> getFutures( Collection<NamedDatabaseId> namedDatabaseIds )
    {
        return namedDatabaseIds.stream()
                .map( id -> reconciliationFutures.get( id.name() ) )
                .flatMap( Stream::ofNullable )
                .collect( Collectors.toList() );
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
