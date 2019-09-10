/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Objects;
import java.util.Optional;

import org.neo4j.kernel.database.DatabaseId;

/**
 * Describes a request to perform a single reconciliation attempt.
 */
public class ReconcilerRequest
{
    private static final ReconcilerRequest SIMPLE = new ReconcilerRequest( false, null, null );
    private static final ReconcilerRequest FORCE = new ReconcilerRequest( true, null, null );

    private final boolean forceReconciliation;
    private final DatabaseId panickedDatabaseId;
    private final Throwable causeOfPanic;

    private ReconcilerRequest( boolean forceReconciliation, DatabaseId panickedDatabaseId, Throwable causeOfPanic )
    {
        this.forceReconciliation = forceReconciliation;
        this.panickedDatabaseId = panickedDatabaseId;
        this.causeOfPanic = causeOfPanic;
    }

    /**
     * A request that does not force state transitions and does not mark any databases as failed.
     *
     * @return a reconciler request.
     */
    public static ReconcilerRequest simple()
    {
        return SIMPLE;
    }

    /**
     * A request that forces state transitions and does not mark any databases as failed.
     *
     * @return a reconciler request.
     */
    public static ReconcilerRequest force()
    {
        return FORCE;
    }

    /**
     * A request that does not force state transitions and marks the specified database as failed.
     *
     * @return a reconciler request.
     */
    public static ReconcilerRequest forPanickedDatabase( DatabaseId databaseId, Throwable causeOfPanic )
    {
        return new ReconcilerRequest( false, databaseId, causeOfPanic );
    }

    /**
     * Whether or not to force the reconciler to try transitions for databases which previously failed.
     *
     * @return {@code true} if transitions should be forced, {@code false} otherwise.
     */
    boolean forceReconciliation()
    {
        return forceReconciliation;
    }

    /**
     * Whether or not the given database panicked and should be marked as failed in {@link DatabaseState} after the reconciliation attempt.
     * Returns an Optional cause, where the *lack* of a cause marks that the give database has not panicked.
     *
     * @return {@code Optional.of( cause )} if the state should be failed, {@code Optional.empty()} otherwise.
     */
    Optional<Throwable> databasePanicked( DatabaseId databaseId )
    {
        boolean thisDatabaseHasPanicked = panickedDatabaseId != null && panickedDatabaseId.equals( databaseId );
        return thisDatabaseHasPanicked ? Optional.of( causeOfPanic ) : Optional.empty();
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
        ReconcilerRequest that = (ReconcilerRequest) o;
        return forceReconciliation == that.forceReconciliation && Objects.equals( panickedDatabaseId, that.panickedDatabaseId ) &&
                Objects.equals( causeOfPanic, that.causeOfPanic );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( forceReconciliation, panickedDatabaseId, causeOfPanic );
    }

    @Override
    public String toString()
    {
        return "ReconcilerRequest{" + "forceReconciliation=" + forceReconciliation + ", panickedDatabaseId=" + panickedDatabaseId + ", causeOfPanic=" +
                causeOfPanic + '}';
    }
}
