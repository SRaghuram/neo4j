/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Objects;
import java.util.Set;

import org.neo4j.kernel.database.DatabaseId;

/**
 * Describes a request to perform a single reconciliation attempt.
 */
public class ReconcilerRequest
{
    private static final ReconcilerRequest SIMPLE = new ReconcilerRequest( false, Set.of() );
    private static final ReconcilerRequest FORCE = new ReconcilerRequest( true, Set.of() );

    private final boolean forceReconciliation;
    private final Set<DatabaseId> databasesToFailAfterReconciliation;

    private ReconcilerRequest( boolean forceReconciliation, Set<DatabaseId> databasesToFailAfterReconciliation )
    {
        this.forceReconciliation = forceReconciliation;
        this.databasesToFailAfterReconciliation = databasesToFailAfterReconciliation;
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
    public static ReconcilerRequest reconcileAndFail( DatabaseId databaseId )
    {
        return new ReconcilerRequest( false, Set.of( databaseId ) );
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
     * Whether or not the given database should be marked as failed in {@link DatabaseReconcilerState} after the reconciliation attempt.
     *
     * @return {@code true} if the state should be failed, {@code false} otherwise.
     */
    boolean shouldFailAfterReconciliation( DatabaseId databaseId )
    {
        return databasesToFailAfterReconciliation.contains( databaseId );
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
               Objects.equals( databasesToFailAfterReconciliation, that.databasesToFailAfterReconciliation );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( forceReconciliation, databasesToFailAfterReconciliation );
    }

    @Override
    public String toString()
    {
        return "ReconcilerRequest{" +
               "forceReconciliation=" + forceReconciliation +
               ", databasesToFailAfterReconciliation=" + databasesToFailAfterReconciliation +
               '}';
    }
}
