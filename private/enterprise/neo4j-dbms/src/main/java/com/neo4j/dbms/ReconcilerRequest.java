/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Map;
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
    private enum RequestType
    {
        PRIORITY, EXPLICIT
    }
    private static final ReconcilerRequest SIMPLE = new ReconcilerRequest( Map.of(), null, null );

    private final Map<String,RequestType> specifiedDatabases;
    private final boolean isSimple;
    private final NamedDatabaseId panickedDatabaseId;
    private final Throwable causeOfPanic;

    private ReconcilerRequest( Map<String,RequestType> specifiedDatabases, NamedDatabaseId panickedDatabaseId, Throwable causeOfPanic )
    {
        this.specifiedDatabases = Map.copyOf( specifiedDatabases );
        this.isSimple = specifiedDatabases.isEmpty();
        this.panickedDatabaseId = panickedDatabaseId;
        this.causeOfPanic = causeOfPanic;
    }

    /**
     * A request that does not specify any database reconciliations as high priority and does not specify any databases as failed.
     *
     * @return a reconciler request.
     */
    public static ReconcilerRequest simple()
    {
        return SIMPLE;
    }

    /**
     * A request that forces state transitions and does not specify any databases as failed.
     *
     * @return a reconciler request.
     * @param priorityDatabase the database to be treated as priority next reconciliation
     */
    public static ReconcilerRequest priority( NamedDatabaseId priorityDatabase )
    {
        return priority( Set.of( priorityDatabase ) );
    }

    /**
     * A request that forces state transitions and ignores previous failures and executes out of order.
     *
     * @return a reconciler request.
     * @param priorityDatabases the subset of databases to be treated as priority next reconciliation
     */
    public static ReconcilerRequest priority( Set<NamedDatabaseId> priorityDatabases )
    {
        var priorityDbNames = priorityDatabases.stream()
                .collect( Collectors.toMap( NamedDatabaseId::name, ignored -> RequestType.PRIORITY ) );
        return new ReconcilerRequest( priorityDbNames, null, null );
    }

    /**
     * A request that forces state transitions and ignores previous failures.
     *
     * @return a reconciler request.
     * @param explicitDatabases the subset of databases to be treated as explicit next reconciliation
     */
    public static ReconcilerRequest explicit( Set<NamedDatabaseId> explicitDatabases )
    {
        var explicitDbNames = explicitDatabases.stream()
                .collect( Collectors.toMap( NamedDatabaseId::name, ignored -> RequestType.EXPLICIT ) );
        return new ReconcilerRequest( explicitDbNames, null, null );
    }

    /**
     * A request that does not force state transitions and marks the specified database as failed.
     *
     * @return a reconciler request.
     */
    public static ReconcilerRequest forPanickedDatabase( NamedDatabaseId namedDatabaseId, Throwable causeOfPanic )
    {
        return new ReconcilerRequest( Map.of( namedDatabaseId.name(), RequestType.PRIORITY ), namedDatabaseId, causeOfPanic );
    }

    /**
     * Set of the databases names which were specified as priority or explicit in this request
     *
     * @return Set of the databases names
     */
    Set<String> specifiedDatabaseNames()
    {
        return specifiedDatabases.keySet();
    }

    /**
     * Whether or not this request considers the reconciliation of the given database Id to be done on a priority executor
     *
     * @return {@code true} if transitions should be executed on a priority executor for databaseId, {@code false} otherwise.
     */
    boolean shouldBeExecutedAsPriorityFor( String databaseName )
    {
        return specifiedDatabases.get( databaseName ) == RequestType.PRIORITY;
    }

    /**
     * Whether or not this request allows the reconciliation of the given database Id if the prevoius transation failed
     *
     * @return {@code true} if transitions may heal previous failure, {@code false} otherwise.
     */
    boolean overridesPreviousFailuresFor( String databaseName )
    {
        return specifiedDatabases.containsKey( databaseName );
    }

    /**
     * Whether or not this request allows the use the result of an already ongoing or queued reconciliation of the given database Id
     *
     * @return {@code true} if cache may be used, {@code false} otherwise.
     */
    boolean canUseCacheFor( String databaseName )
    {
        return !specifiedDatabases.containsKey( databaseName );
    }

    /**
     * Whether or not the given database panicked and should be specified as failed in {@link EnterpriseDatabaseState} after the reconciliation attempt.
     * Returns an Optional cause, where the *lack* of a cause specifies that the give database has not panicked.
     *
     * @return {@code Optional.of( cause )} if the state should be failed, {@code Optional.empty()} otherwise.
     */
    Optional<Throwable> causeOfPanic( NamedDatabaseId namedDatabaseId )
    {
        boolean thisDatabaseHasPanicked = causeOfPanic != null && Objects.equals( namedDatabaseId, panickedDatabaseId );
        return thisDatabaseHasPanicked ? Optional.of( causeOfPanic ) : Optional.empty();
    }

    /**
     * @return Whether this reconciler request is simple (i.e. isn't high priority  or explicit for any databases and has no panic cause associated).
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
        return isSimple == that.isSimple && Objects.equals( specifiedDatabases, that.specifiedDatabases ) &&
               Objects.equals( panickedDatabaseId, that.panickedDatabaseId ) && Objects.equals( causeOfPanic, that.causeOfPanic );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( specifiedDatabases, isSimple, panickedDatabaseId, causeOfPanic );
    }

    @Override
    public String toString()
    {
        return "ReconcilerRequest{" + "specifiedDatabases=" + specifiedDatabases + ", isSimple=" + isSimple + ", panickedDatabaseId=" + panickedDatabaseId +
               ", causeOfPanic=" + causeOfPanic + '}';
    }
}
