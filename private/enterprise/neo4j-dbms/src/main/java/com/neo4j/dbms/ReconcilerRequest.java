/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;

/**
 * Describes a request to perform a single reconciliation attempt.
 */
public class ReconcilerRequest
{
    private static final ReconcilerRequest SIMPLE = new ReconcilerRequest( false, null );
    private static final ReconcilerRequest FORCE = new ReconcilerRequest( true, null );

    private final boolean forceReconciliation;
    private final DatabaseId panickedDatabaseId;

    private ReconcilerRequest( boolean forceReconciliation, DatabaseId panickedDatabaseId )
    {
        this.forceReconciliation = forceReconciliation;
        this.panickedDatabaseId = panickedDatabaseId;
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
    public static ReconcilerRequest forPanickedDatabase( DatabaseId databaseId )
    {
        return new ReconcilerRequest( false, databaseId );
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
     *
     * @return {@code true} if the state should be failed, {@code false} otherwise.
     */
    boolean databasePanicked( DatabaseId databaseId )
    {
        return panickedDatabaseId != null && panickedDatabaseId.equals( databaseId );
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
        var that = (ReconcilerRequest) o;
        return forceReconciliation == that.forceReconciliation &&
               Objects.equals( panickedDatabaseId, that.panickedDatabaseId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( forceReconciliation, panickedDatabaseId );
    }

    @Override
    public String toString()
    {
        return "ReconcilerRequest{" +
               "forceReconciliation=" + forceReconciliation +
               ", panickedDatabaseId=" + panickedDatabaseId +
               '}';
    }
}
