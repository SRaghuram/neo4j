/*
 * Copyright (c) "Neo4j"
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

import static java.util.function.Function.identity;

/**
 * Describes a request to perform a single reconciliation attempt.
 *
 * Reconciliation attempts are *always* global. All requests will cause the reconciler to touch all databases.
 * This model is chosen for simplicity, and is relatively low cost as redundant reconciliations (where a database is already in its desired state)
 * are no-ops.
 *
 * A request may specify a subset of databases as normal or high prioirity "targets". This has a few effects:
 *   - Even if those databases have failed a previous transition, reconciliation will be re-attempted to bring them to their desired states.
 *   - If high priority, reconciliation jobs are handled by a different, unbounded thread pool, and therefore ignore the reconciler's parallelism limit.
 *   - Databases not specified as targets are *still* reconciled, following the same rules as {@code ReconcilerRequest.simple()}
 *
 * Note that internally the reconciler tracks databases as Strings because a single reconciliation job may operate on databases with two different ids
 * but the same name ( i.e. STOPPED->DROPPED->INITIAL->STARTED ). The job as a whole should still be priority.
 *                         |----(foo,1)----|  |----(foo,2)----|
 */
public final class ReconcilerRequest
{
    private enum RequestPriority
    {
        HIGH, NORMAL
    }

    private static final ReconcilerRequest SIMPLE = new ReconcilerRequest( Map.of(), null, null );

    private final Map<String,RequestPriority> targetDatabases;
    private final NamedDatabaseId panickedDatabaseId;
    private final Throwable causeOfPanic;

    private ReconcilerRequest( Map<String,RequestPriority> targetDatabases, NamedDatabaseId panickedDatabaseId, Throwable causeOfPanic )
    {
        this.targetDatabases = Map.copyOf( targetDatabases );
        this.panickedDatabaseId = panickedDatabaseId;
        this.causeOfPanic = causeOfPanic;
    }

    /**
     * A request that does not target any database in particular. When triggered with such a request, a reconciler
     * merely attempts to reconcile those databases which have not failed, and are not in their desired state.
     *
     * @return a reconciler request.
     */
    public static ReconcilerRequest simple()
    {
        return SIMPLE;
    }

    /**
     * Builds a request that may ignore previous reconciliation failures for the target databases.
     * Databases may be the targets of reconciler requests when they are explicitly updated by a user
     * (e.g. via the system graph and accompanying {@link SystemGraphDbmsOperator}).
     *
     * @return a populated reconciler request builder.
     * @param databases the subset of databases to be treated as explicit targets of the next reconciliation
     */
    public static HasExplicitTargets targets( Set<NamedDatabaseId> databases )
    {
        return new Builder( databases, Set.of() );
    }

    /**
     * Builds a request that may ignore previous reconciliation failures for the target databases.
     * These requests will also be given priority when waiting to acquire locks, and are not subject
     * to the normal parallelism limits of the reconciler, so are less likely to spend time waiting.
     *
     * Databases may be priority targets of reconciler requests when they are updated by a privileged
     * user, or internal operator (e.g. via {@link ShutdownOperator}).
     *
     * @return a populated reconciler request builder.
     * @param priorityDatabase the database to be treated as priority targets of the next reconciliation
     */
    public static HasPriorityTargets priorityTarget( NamedDatabaseId priorityDatabase )
    {
        return priorityTargets( Set.of( priorityDatabase ) );
    }

    /**
     * Builds a request that may ignore previous reconciliation failures for the target databases.
     * These requests will also be given priority when waiting to acquire locks, and are not subject
     * to the normal parallelism limits of the reconciler, so are less likely to spend time waiting.
     *
     * Databases may be priority targets of reconciler requests when they are updated by a privileged
     * user, or internal operator (e.g. via {@link ShutdownOperator}).
     *
     * @return a populated reconciler request builder.
     * @param priorityDatabases the subset of databases to be treated as priority targets of the next reconciliation
     */
    public static HasPriorityTargets priorityTargets( Set<NamedDatabaseId> priorityDatabases )
    {
        return new Builder( Set.of(), priorityDatabases );
    }

    /**
     * Builds a special reconciler request that marks the specified database as failed with the provided cause.
     * The request considers the provided database as a high priority target, allowing the reconciler to force
     * an attempt at transitioning it to a STOPPED state.
     *
     * @return a populated reconciler request builder.
     */
    public static NeedsBuild panickedTarget( NamedDatabaseId namedDatabaseId, Throwable causeOfPanic )
    {
        return new Builder( namedDatabaseId, causeOfPanic );
    }

    /**
     * Set of the databases names which were specified as priority or explicit in this request
     *
     * @return Set of the databases names
     */
    Set<String> explicitTargets()
    {
        return targetDatabases.keySet();
    }

    /**
     * Whether or not this request considers the reconciliation of the given database Id to be done on a priority executor
     *
     * @return {@code true} if transitions should be executed on a priority executor for databaseId, {@code false} otherwise.
     */
    boolean shouldBeExecutedAsPriorityFor( String databaseName )
    {
        return targetDatabases.get( databaseName ) == RequestPriority.HIGH;
    }

    /**
     * Whether or not this request allows the reconciliation of the given database Id if the prevoius transation failed
     *
     * @return {@code true} if transitions may heal previous failure, {@code false} otherwise.
     */
    boolean overridesPreviousFailuresFor( String databaseName )
    {
        return targetDatabases.containsKey( databaseName );
    }

    /**
     * Whether or not this request allows will accept the result of an already waiting reconciliation for the given database
     * Returns true if the given database was *not* an explicit target of this request.
     *
     * @return {@code true} if cache may be used, {@code false} otherwise.
     */
    boolean canUseCacheFor( String databaseName )
    {
        return !targetDatabases.containsKey( databaseName );
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
     * @return Whether this reconciler request is simple (i.e. doesn't specify any particular databases as targets).
     */
    boolean isSimple()
    {
        return targetDatabases.isEmpty();
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
        return Objects.equals( targetDatabases, that.targetDatabases ) &&
               Objects.equals( panickedDatabaseId, that.panickedDatabaseId ) &&
               Objects.equals( causeOfPanic, that.causeOfPanic );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( targetDatabases, panickedDatabaseId, causeOfPanic );
    }

    @Override
    public String toString()
    {
        return "ReconcilerRequest{" + "targetDatabases=" + targetDatabases + ", isSimple=" + isSimple() + ", panickedDatabaseId=" + panickedDatabaseId +
               ", causeOfPanic=" + causeOfPanic + '}';
    }

    static class Builder implements HasPriorityTargets, HasExplicitTargets, NeedsBuild
    {
        private Set<NamedDatabaseId> normalTargets;
        private Set<NamedDatabaseId> priorityTargets;
        private NamedDatabaseId panickedDatabase;
        private Throwable causeOfPanic;

        private Builder( NamedDatabaseId panickedDatabase, Throwable causeOfPanic )
        {
            this.causeOfPanic = causeOfPanic;
            this.panickedDatabase = panickedDatabase;
            this.normalTargets = Set.of();
            this.priorityTargets = Set.of( panickedDatabase );
        }

        private Builder( Set<NamedDatabaseId> normalTargets, Set<NamedDatabaseId> priorityTargets )
        {
            this.normalTargets = normalTargets;
            this.priorityTargets = priorityTargets;
        }

        @Override
        public NeedsBuild targets( Set<NamedDatabaseId> databases )
        {
            normalTargets = databases;
            return this;
        }

        @Override
        public NeedsBuild priorityTargets( Set<NamedDatabaseId> databases )
        {
            priorityTargets = databases;
            return this;
        }

        @Override
        public ReconcilerRequest build()
        {
            var targets = normalTargets.stream()
                                       .map( NamedDatabaseId::name )
                                       .distinct()
                                       .collect( Collectors.toMap( identity(), ignored -> RequestPriority.NORMAL ) );
            var prioTargets = priorityTargets.stream()
                                             .map( NamedDatabaseId::name )
                                             .distinct()
                                             .collect( Collectors.toMap( identity(), ignored -> RequestPriority.HIGH ) );

            targets.putAll( prioTargets );
            return new ReconcilerRequest( targets, panickedDatabase, causeOfPanic );
        }
    }

    interface NeedsBuild
    {
        ReconcilerRequest build();
    }

    interface HasPriorityTargets extends NeedsBuild
    {
        NeedsBuild targets( Set<NamedDatabaseId> databases );
    }

    interface HasExplicitTargets extends NeedsBuild
    {
        NeedsBuild priorityTargets( Set<NamedDatabaseId> databases );
    }
}
