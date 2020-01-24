/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.kernel.database.NamedDatabaseId;

/**
 * Describes a request to perform a single reconciliation attempt.
 *
 * Reconciliation attempts are *always* global. All requests will cause the reconciler to touch all databases.
 * This model is chosen for simplicity, and is relatively low cost as redundant reconciliations (where a database is already in its desired state)
 * are no-ops.
 *
 * A reconciler request may specify a subset of databases as being high priority. This has a couple of effects:
 *   - Even if those databases have failed a previous transition, reconciliation will be re-attempted to bring them to their desired states.
 *   - High priority reconciliation jobs are handled by a different, unbounded thread pool, and therefore ignore the reconciler's parallelism limit.
 *
 *  Note that internally the reconciler tracks databases as Strings because a single reconciliation job may operate on databases with two different ids
 *  but the same name ( i.e. STOPPED->DROPPED->INITIAL->STARTED ). The job as a whole should still be priority.
 *                          |----(foo,1)----|  |----(foo,2)----|
 */
public final class ReconcilerRequest
{
    private static final ReconcilerRequest SIMPLE = new ReconcilerRequest( Set.of(), null, null );

    private final Set<String> priorityDatabases;
    private final boolean isSimple;
    private final NamedDatabaseId panickedDatabaseId;
    private final Throwable causeOfPanic;

    private ReconcilerRequest( Set<String> namesOfPriorityDatabases, NamedDatabaseId panickedDatabaseId, Throwable causeOfPanic )
    {
        this.priorityDatabases = namesOfPriorityDatabases;
        this.isSimple = namesOfPriorityDatabases.isEmpty();
        this.panickedDatabaseId = panickedDatabaseId;
        this.causeOfPanic = causeOfPanic;
    }

    /**
     * A request that does not mark any database reconciliations as high priority and does not mark any databases as failed.
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
     * @param priorityDatabase the database to be treated as priority next reconciliation
     */
    public static ReconcilerRequest priority( NamedDatabaseId priorityDatabase )
    {
        return priority( Set.of( priorityDatabase ) );
    }

    /**
     * A request that forces state transitions and does not mark any databases as failed.
     *
     * @return a reconciler request.
     * @param priorityDatabases the subset of databases to be treated as priority next reconciliation
     */
    public static ReconcilerRequest priority( Set<NamedDatabaseId> priorityDatabases )
    {
        var priorityDbNames = priorityDatabases.stream()
                .map( NamedDatabaseId::name )
                .collect( Collectors.toSet() );
        return new ReconcilerRequest( priorityDbNames, null, null );
    }

    /**
     * A request that does not force state transitions and marks the specified database as failed.
     *
     * @return a reconciler request.
     */
    public static ReconcilerRequest forPanickedDatabase( NamedDatabaseId namedDatabaseId, Throwable causeOfPanic )
    {
        return new ReconcilerRequest( Set.of( namedDatabaseId.name() ), namedDatabaseId, causeOfPanic );
    }

    Set<String> priorityDatabaseNames()
    {
        return new HashSet<>( priorityDatabases );
    }

    /**
     * Whether or not this request considers the reconciliation of the given database Id to be high priority.
     *
     * @return {@code true} if transitions for databaseId should be high priority, {@code false} otherwise.
     */
    boolean isPriorityRequestForDatabase( String databaseName )
    {
        return priorityDatabases.contains( databaseName );
    }

    /**
     * Whether or not the given database panicked and should be marked as failed in {@link EnterpriseDatabaseState} after the reconciliation attempt.
     * Returns an Optional cause, where the *lack* of a cause marks that the give database has not panicked.
     *
     * @return {@code Optional.of( cause )} if the state should be failed, {@code Optional.empty()} otherwise.
     */
    Optional<Throwable> causeOfPanic( NamedDatabaseId namedDatabaseId )
    {
        boolean thisDatabaseHasPanicked = causeOfPanic != null && Objects.equals( namedDatabaseId, panickedDatabaseId );
        return thisDatabaseHasPanicked ? Optional.of( causeOfPanic ) : Optional.empty();
    }

    /**
     * @return Whether this reconciler request is simple (i.e. isn't high priority for any databases and has no panic cause associated).
     */
    boolean isSimple()
    {
        return isSimple;
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
        return isSimple == that.isSimple && Objects.equals( priorityDatabases, that.priorityDatabases ) &&
                Objects.equals( panickedDatabaseId, that.panickedDatabaseId ) && Objects.equals( causeOfPanic, that.causeOfPanic );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( priorityDatabases, isSimple, panickedDatabaseId, causeOfPanic );
    }

    @Override
    public String toString()
    {
        return "ReconcilerRequest{" + "priorityDatabases=" + priorityDatabases + ", isSimple=" + isSimple + ", panickedDatabaseId=" + panickedDatabaseId +
                ", causeOfPanic=" + causeOfPanic + '}';
    }
}
