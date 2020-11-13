/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.MultiDatabaseManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.internal.helpers.DefaultTimeoutStrategy.exponential;

/**
 * Responsible for controlling the lifecycles of *all* databases in this neo4j instance, via the {@link DatabaseManager}.
 * As opposed to the imperative interface provided by a DatabaseManager, the reconciler offers a declarative interface
 * whereby a user, or internal system components, declare the state (see {@link EnterpriseOperatorState}) they *desire* a database
 * to be in. The reconciler then calculates the sequence of steps that the DatabaseManager must execute to bring the
 * database in question to that state.
 *
 * Sequences of reconciler steps for different databases will be executed in parallel, whilst steps for the same database
 * will be executed strictly sequentially. In fact, each state transition (all their resulting steps) are executed sequentially
 * for a given database.
 *
 * For example, if a user requests (via the `{@link SystemGraphDbmsOperator}`) that the STOPPED database foo is STARTED and
 * shortly afterward requests that foo is DROPPED, the reconciler will block the thread performing the DROPPED transition
 * until the STARTED transition has successfully finished. Also, the sequence of steps required for the DROPPED transition is
 * calculated only once the thread is unblocked. In other words the transition required is STARTED->STOPPED->DROPPED,
 * rather than just STOPPED->DROPPED.
 *
 * Besides parallelisation, the reconciler provides optional backoff/retry semantics for failed state transitions.
 *
 * Users, internal components, extensions etc... can declare the desired state of a given database via various {@link DbmsOperator}
 * instances. When triggered, the reconciler fetches the desired states from each operator as {@code Map<String,EnterpriseDatabaseState>}
 * and merges them. When multiple operators specify a desired state for the same database, one state is chosen according to the
 * {@code this.precedence} binary operator. The final merged map of DatabaseIds to OperatorStates is compared to {this.currentStates}
 * and changes are made where necessary.
 *
 * Note: With the exception of short lived instances, sometimes created as a side effect of creating a database, reconcilers are global singletons.
 */
public class DbmsReconciler
{
    private final TimeoutStrategy strategy;
    private final ReconcilerExecutors executors;
    private final Map<String,CompletableFuture<ReconcilerStepResult>> waitingJobCache;

    private final MultiDatabaseManager<? extends DatabaseContext> databaseManager;
    private final BinaryOperator<EnterpriseDatabaseState> precedence;
    protected final Log log;
    private final boolean canRetry;
    private final Map<String,EnterpriseDatabaseState> currentStates;
    private final TransitionsTable transitionsTable;
    private final List<DatabaseStateChangedListener> listeners;
    private final ReconcilerLocks locks;

    DbmsReconciler( MultiDatabaseManager<? extends DatabaseContext> databaseManager, Config config, LogProvider logProvider, JobScheduler scheduler,
            TransitionsTable transitionsTable )
    {
        this.databaseManager = databaseManager;

        this.canRetry = config.get( GraphDatabaseSettings.reconciler_may_retry );
        this.strategy = exponential( config.get( GraphDatabaseSettings.reconciler_minimum_backoff ).toMillis(),
                                     config.get( GraphDatabaseSettings.reconciler_maximum_backoff ).toMillis(),
                                     MILLISECONDS );

        this.executors = new ReconcilerExecutors( scheduler, config );
        this.locks = new ReconcilerLocks();
        this.currentStates = new ConcurrentHashMap<>();
        this.waitingJobCache = new ConcurrentHashMap<>();
        this.log = logProvider.getLog( getClass() );
        this.precedence = EnterpriseOperatorState.minByOperatorState( EnterpriseDatabaseState::operatorState );
        this.transitionsTable = transitionsTable;
        this.listeners = new CopyOnWriteArrayList<>();
    }

    ReconcilerResult reconcile( List<DbmsOperator> operators, ReconcilerRequest request )
    {
        var namesOfDbsToReconcile = operators.stream()
                .flatMap( op -> op.desired().keySet().stream() )
                .collect( Collectors.toSet() );

        validatedAndWarn( request, namesOfDbsToReconcile );
        reportFailedDatabases();

        var reconciliation = namesOfDbsToReconcile.stream()
                .map( dbName -> Pair.of( dbName, scheduleReconciliationJob( dbName, request, operators ) ) )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );

        return new ReconcilerResult( reconciliation );
    }

    private void reportFailedDatabases()
    {
        var failedDbs = currentStates.values().stream()
                .filter( EnterpriseDatabaseState::hasFailed )
                .map( db -> db.databaseId().name() )
                .collect( Collectors.joining( ",", "[", "]" ) );

        if ( failedDbs.length() > 2 )
        {
            log.warn( "Reconciler triggered but the following databases are currently failed and may be ignored: %s. " +
                    "Run `SHOW DATABASES` for further information.", failedDbs );
        }
    }

    private void validatedAndWarn( ReconcilerRequest request, Set<String> namesOfDbsToReconcile )
    {
        if ( !request.isSimple() )
        {
            var requestedDbs = new HashSet<>( request.explicitTargets() );
            requestedDbs.removeAll( namesOfDbsToReconcile );

            if ( !requestedDbs.isEmpty() )
            {
                log.warn( "Reconciliation request specifies unknown databases: [%s]. Reconciler is tracking: [%s]",
                        requestedDbs, namesOfDbsToReconcile );
            }
        }
    }

    private static Map<String,EnterpriseDatabaseState> combineDesiredStates( Map<String,EnterpriseDatabaseState> combined,
            Map<String,EnterpriseDatabaseState> operator, BinaryOperator<EnterpriseDatabaseState> precedence )
    {
        return Stream.concat( combined.entrySet().stream(), operator.entrySet().stream() )
                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue, precedence ) );
    }

    private static Map<String,EnterpriseDatabaseState> desiredStates( List<DbmsOperator> operators, BinaryOperator<EnterpriseDatabaseState> precedence )
    {
        return operators.stream()
                .map( DbmsOperator::desired )
                .reduce( new HashMap<>(), ( l, r ) -> DbmsReconciler.combineDesiredStates( l, r, precedence ) );
    }

    EnterpriseDatabaseState getReconcilerEntryOrDefault( NamedDatabaseId namedDatabaseId, Supplier<EnterpriseDatabaseState> initial )
    {
        return Optional.ofNullable( currentStates.get( namedDatabaseId.name() ) ).orElseGet( initial );
    }

    protected EnterpriseDatabaseState initialReconcilerEntry( NamedDatabaseId namedDatabaseId )
    {
        return EnterpriseDatabaseState.initial( namedDatabaseId );
    }

    /**
     * This method attempts to perform reconciliation of the given database to the desired state, as specified via {@link DbmsOperator#desired()} for each of
     * the provided operators.
     *
     * Reconciliation happens in 4 distinct steps: ACQUIRE LOCKS -> RECONCILE -> RETRY -> RELEASE LOCKS
     *
     * Each of these steps is asynchronously executed on completion of the previous step. Unfortunately, {@link CompletableFuture#thenCompose(Function)} does
     * not guarantee that the submitted function is executed using the same execution context as the previous step. Therefore, in order to ensure
     * reconciliation steps are strictly limited to being executed on the reconciler's provided executor, we must issue each step function using
     * {@link CompletableFuture#supplyAsync(Supplier, Executor)} again.
     *
     * As a result, a reconciliation job is implemented as a stack of futures, implemented broadly as follows:
     *
     * {@code
     *      CompletableFuture.supplyAsync( step, executor )
     *          .thenCompose( CompletableFuture.supplyAsync( step, executor )
     *              .thenCompose( CompletableFuture.supplyAsync( step, executor )
     *                  .andSoOn() ) }
     *
     * ... though this is factored out across several methods, for clarity.
     *
     * Note that we also cache simple reconciliation jobs: reconciliation jobs which neither panic/fail the database, nor force reconciler to perform
     * transitions on failed databases.
     *
     * As the desired state of each database is calculated dynamically when a reconciliation actually starts
     * (after acquiring a lock), if a job is triggered for a database whilst another is already waiting, we just give the caller a reference
     * to the waiting job. Any changes to the database's desired state which caused that additional call to
     * {@link DbmsReconciler#reconcile(List, ReconcilerRequest)} will be picked up by the earlier waiting job when it finally starts reconciling.
     */
    private synchronized CompletableFuture<ReconcilerStepResult> scheduleReconciliationJob( String databaseName, ReconcilerRequest request,
            List<DbmsOperator> operators )
    {
        var jobCanBeCached = request.canUseCacheFor( databaseName );
        if ( jobCanBeCached )
        {
            var cachedJob = waitingJobCache.get( databaseName );
            if ( cachedJob != null )
            {
                return cachedJob;
            }
        }

        var reconcilerJobHandle = new CompletableFuture<Void>();
        // Whilst the transitions step may take place on either the bound or unbound executor depending on the request
        //  the preReconcile step will always take place on the unbound executor, as threads in this step only wait to
        //  acquire locks.
        var preReconcileExecutor = executors.unboundExecutor();

        var job = reconcilerJobHandle
                .thenCompose( ignored -> preReconcile( databaseName, operators, request, preReconcileExecutor ) )
                .thenCompose( desiredState -> doTransitions( databaseName, desiredState, request ) )
                .whenComplete( ( result, throwable ) -> postReconcile( databaseName, request, result, throwable ) );

        if ( jobCanBeCached )
        {
            waitingJobCache.put( databaseName, job );
        }

        //Having synchronously placed the job future in the cache (and thus avoiding potential races)
        // we can now start the job by completing the Void handle future at the head of the chain.
        reconcilerJobHandle.complete( null );
        return job;
    }

    private CompletableFuture<EnterpriseDatabaseState> preReconcile( String databaseName, List<DbmsOperator> operators, ReconcilerRequest request,
            Executor executor )
    {
        Supplier<EnterpriseDatabaseState> preReconcileJob = () ->
        {
            try
            {
                log.debug( "Attempting to acquire lock before reconciling state of database `%s`.", databaseName );
                locks.acquireLockOn( request, databaseName );

                if ( request.canUseCacheFor( databaseName ) )
                {
                    // Must happen-before extracting desired states otherwise the cache might return a job which reconciles to an
                    // earlier desired state than that specified by the component triggering this job.
                    waitingJobCache.remove( databaseName );
                }

                var desiredState = desiredStates( operators, precedence ).get( databaseName );
                if ( desiredState == null )
                {
                    throw new IllegalStateException( format( "No operator desires a state for database %s any more. " +
                            "This is likely an error!", databaseName ) );
                }
                else if ( !Objects.equals( databaseName, desiredState.databaseId().name() ) )
                {
                    throw new IllegalStateException( format( "The supplied database name %s does not match that stored " +
                            "in its desired state %s!", databaseName, desiredState.databaseId().name() ) );
                }
                return desiredState;
            }
            catch ( InterruptedException e )
            {
                currentThread().interrupt();
                throw new CompletionException( e );
            }

        };
        return CompletableFuture.supplyAsync( () -> namedJob( databaseName, preReconcileJob ), executor );
    }

    private CompletableFuture<ReconcilerStepResult> doTransitions( String databaseName, EnterpriseDatabaseState desiredState, ReconcilerRequest request )
    {
        var currentState = getReconcilerEntryOrDefault( desiredState.databaseId(), () -> initialReconcilerEntry( desiredState.databaseId() ) );
        var initialResult = new ReconcilerStepResult( currentState, null, desiredState );

        if ( currentState.equals( desiredState ) )
        {
            return CompletableFuture.completedFuture( initialResult );
        }
        if ( currentState.hasFailed() && !request.overridesPreviousFailuresFor( databaseName ) )
        {
            log.debug( "Request to transition database %s from %s to %s is denied. " +
                       "A previous transition failed and request did not specify to heal the failure", databaseName, currentState, desiredState );
            var previousError = currentState.failure().orElseThrow( IllegalStateException::new );
            return CompletableFuture.completedFuture( initialResult.withError( DatabaseManagementException.wrap( previousError ) ) );
        }
        log.info( "Database '%s' is requested to transition %s", databaseName, EnterpriseDatabaseState.logFromTo( currentState, desiredState ) );

        var backoff = strategy.newTimeout();
        var steps = getLifecycleTransitionSteps( currentState, desiredState );
        var executor = executors.executor( request, desiredState.databaseId() );

        var namedDatabaseId = desiredState.databaseId();
        return CompletableFuture.supplyAsync( () -> doTransitions( initialResult.state(), steps, desiredState ), executor )
                .thenCompose( result -> handleResult( namedDatabaseId, desiredState, result, executor, backoff, 0 ) );
    }

    private static ReconcilerStepResult doTransitions( EnterpriseDatabaseState currentState,
                                                       Stream<Transition.Prepared> steps,
                                                       EnterpriseDatabaseState desiredState )
    {
        Supplier<ReconcilerStepResult> job = () -> doTransitionStep( steps.iterator(), new ReconcilerStepResult( currentState, null, desiredState ) );
        return namedJob( desiredState.databaseId().name(), job );
    }

    private static <T> T namedJob( String name, Supplier<T> operation )
    {

        String oldThreadName = currentThread().getName();
        try
        {
            currentThread().setName( oldThreadName + "-" + name );
            return operation.get();
        }
        finally
        {
            currentThread().setName( oldThreadName );
        }
    }

    private static ReconcilerStepResult doTransitionStep( Iterator<Transition.Prepared> steps, ReconcilerStepResult result )
    {
        if ( !steps.hasNext() )
        {
            return result;
        }

        try
        {
            var preparedTransition = steps.next();
            try
            {
                var nextState = preparedTransition.doTransition();
                return doTransitionStep( steps, result.withState( nextState ) );
            }
            catch ( TransitionFailureException failure )
            {
                return doTransitionCleanupStep( preparedTransition, failure, result );
            }
        }
        catch ( Throwable throwable )
        {
            // This is last line of defense:
            //   We think the two scenarios here where we would get a Throwable which isn't wrapped in a TransitionFailure are:
            //   - The assignment of nextState is the "hair that broke the camel's back" and throws and OOM.
            //   - There are way too many steps and doTransitionStep eventually throws a StackOverflowError.
            //   Since in this case we don't know what would have been the failed state we leave the database in the last state it successfully reached
            //   and just set that to failed with the catched Throwable
            return result.withError( throwable );
        }
    }

    private static ReconcilerStepResult doTransitionCleanupStep( Transition.Prepared preparedTransition,
            TransitionFailureException originalFailure, ReconcilerStepResult result )
    {
        try
        {
            preparedTransition.doCleanup();
        }
        catch ( TransitionFailureException actualFailure )
        {
            originalFailure.getCause().addSuppressed( actualFailure.getCause() );
        }
        return result.withState( originalFailure.failedState() ).withError( originalFailure.getCause() );
    }

    private CompletableFuture<ReconcilerStepResult> handleResult( NamedDatabaseId namedDatabaseId, EnterpriseDatabaseState desiredState,
            ReconcilerStepResult result, Executor executor, TimeoutStrategy.Timeout backoff, int retries )
    {
        boolean isFatalError = result.error() != null && isFatalError( result.error() );
        if ( result.error() == null || isFatalError )
        {
            if ( (result.error() == null) && desiredState.failure().isPresent() )
            {
                return CompletableFuture.completedFuture( result.withState( desiredState ) );
            }
            return CompletableFuture.completedFuture( result );
        }

        var attempt = retries + 1;
        log.warn( "Retrying reconciliation of %s to state '%s'. This is attempt %d.", namedDatabaseId,
                desiredState.operatorState().description(), attempt );

        var remainingSteps = getLifecycleTransitionSteps( result.state(), desiredState );
        return CompletableFuture.supplyAsync( () -> doTransitions( result.state(), remainingSteps, desiredState ),
                delayedExecutor( backoff.getAndIncrement(), TimeUnit.MILLISECONDS, executor ) )
                .thenCompose( retryResult -> handleResult( namedDatabaseId, desiredState, retryResult, executor, backoff, attempt ) );
    }

    private void postReconcile( String databaseName, ReconcilerRequest request, ReconcilerStepResult result, Throwable throwable )
    {
        try
        {
            currentStates.compute( databaseName, ( name, previousState ) ->
            {
                var failedState = handleReconciliationErrors( throwable, request, result, databaseName, previousState );
                // failedState.isEmpty() and result == null cannot both be true, *however* the method reference form result::state cannot be used here
                // as the left hand side of the :: is evaluated (and NPE thrown) even if the supplier itself is never called.
                // noinspection Convert2MethodRef
                var nextState = failedState.orElseGet( () -> result.state() );
                stateChanged( previousState, nextState );
                return nextState;
            } );
        }
        finally
        {
            locks.releaseLockOn( databaseName );
            var errorExists = throwable != null || result.error() != null;
            var outcome = errorExists ? "failed" : "succeeded";
            var targetState = result == null ? "unknown" : result.desiredState().operatorState().description();
            log.debug( "Released lock having %s to reconcile database `%s` to state %s.", outcome, databaseName, targetState );
        }
    }

    private void stateChanged( EnterpriseDatabaseState previousState, EnterpriseDatabaseState newState )
    {
        var initialState = new EnterpriseDatabaseState( newState.databaseId(), EnterpriseOperatorState.INITIAL );
        if ( !newState.equals( previousState ) )
        {
            log.info( "Database '%s' transition is complete %s", newState.databaseId().name(),
                    EnterpriseDatabaseState.logFromTo( ( previousState == null ) ? initialState : previousState, newState ) );
        }
        // If the previous state has a different id then a drop-recreate must have occurred
        // In this case we should fire the listener twice, once for each databaseId.
        if ( previousState != null && !Objects.equals( previousState.databaseId(), newState.databaseId() ) )
        {
            var droppedPrevious = new EnterpriseDatabaseState( previousState.databaseId(), DROPPED );
            listeners.forEach( listener -> listener.stateChange( previousState, droppedPrevious ) );
            listeners.forEach( listener -> listener.stateChange( initialState, newState ) );
        }
        else
        {
            listeners.forEach( listener -> listener.stateChange( ( previousState == null ) ? initialState : previousState, newState ) );
        }
    }

    private Optional<EnterpriseDatabaseState> handleReconciliationErrors( Throwable throwable, ReconcilerRequest request, ReconcilerStepResult result,
            String databaseName, EnterpriseDatabaseState previousState )
    {
        if ( throwable != null )
        {
            return handleUnexpectedException( throwable, result, databaseName, previousState );
        }
        else if ( result.error() != null && Exceptions.contains( result.error(), e -> e instanceof DatabaseStartAbortedException ) )
        {
            // The current transition was aborted because some internal component detected that the desired state of this database has changed underneath us
            var message = format( "Attempt to reconcile database %s from %s to %s was aborted, likely due to %s being stopped or dropped meanwhile.",
                    databaseName, result.state(), result.desiredState().operatorState().description(), databaseName );
            log.warn( message );
            return Optional.of( result.state() );
        }
        else if ( result.error() != null )
        {
            return handleExpectedException( request, result, databaseName, previousState );
        }
        else
        {
            // No exception occurred during this transition, but the request may still be for a panicked database.
            //   In that case we should mark it as failed anyway
            return handleNoReconciliationErrors( request, result, databaseName, previousState );
        }
    }

    private Optional<EnterpriseDatabaseState> handleUnexpectedException( Throwable throwable, ReconcilerStepResult result, String databaseName,
            EnterpriseDatabaseState previousState )
    {
        // An exception which was not wrapped in a DatabaseManagementException has occurred. E.g. we looked up an unknown state transition or there was
        // an InterruptedException in the reconciler job itself
        EnterpriseDatabaseState state;
        if ( result == null )
        {
            state = (previousState == null ) ? EnterpriseDatabaseState.initialUnknownId() : previousState;
        }
        else
        {
            state = result.state();
        }
        state.failure().ifPresent( throwable::addSuppressed );
        log.error( format( "Encountered unexpected error when attempting to reconcile database %s", databaseName ), throwable );
        return Optional.of( state.failed( throwable ) );
    }

    private Optional<EnterpriseDatabaseState> handleExpectedException( ReconcilerRequest request, ReconcilerStepResult result, String databaseName,
            EnterpriseDatabaseState previousState )
    {
        var currentError = result.error();
        var panicked = request.causeOfPanic( result.state().databaseId() );
        if ( panicked.isPresent() )
        {
            panicked.get().addSuppressed( currentError );
            currentError = panicked.get();
        }
        if ( isErrorNew( previousState, currentError ) )
        {
            // An exception occurred somewhere in the internal machinery of the database and was caught by the DatabaseManager
            var message = format( "Encountered error when attempting to reconcile database %s to state '%s', database remains in state '%s'",
                    databaseName, result.desiredState().operatorState().description(), result.state().operatorState().description() );
            log.error( message, currentError );
        }
        return Optional.of( result.state().failed( currentError ) );
    }

    private static boolean isErrorNew( EnterpriseDatabaseState previousState, Throwable currentError )
    {
        return previousState == null || !Objects.equals( currentError, previousState.failure().orElse( null ) );
    }

    private Optional<EnterpriseDatabaseState> handleNoReconciliationErrors( ReconcilerRequest request, ReconcilerStepResult result, String databaseName,
            EnterpriseDatabaseState previousState )
    {
        var nextState = result.state();
        var panicked = request.causeOfPanic( nextState.databaseId() );
        var canHeal = request.overridesPreviousFailuresFor( databaseName );
        var currentlyFailed = previousState != null && previousState.hasFailed();

        if ( panicked.isPresent() )
        {
            // Cause of panic is not logged because it was surely done before
            var message = format( "Panicked database %s was reconciled to state '%s'",
                    databaseName, result.state().operatorState().description() );
            log.warn( message );
            return panicked.map( nextState::failed );
        }
        else if ( currentlyFailed && !canHeal )
        {
            // Preserve the previous failed state because the reconcile request is not a priority or explicit one
            return previousState.failure().map( nextState::failed );
        }
        else
        {
            // Either the previous state is not failed, or the request allows heal from failure, and therefore may override previous failed states
            return Optional.empty();
        }
    }

    /**
     * A reconciliation error is considered to be fatal/non-retryable if
     * 1) retries are disabled
     * OR 2) the error is e.g. an OOM
     * OR 3) the error is due to a database start which was aborted
     */
    private boolean isFatalError( Throwable t )
    {
        return !canRetry || t instanceof Error || t instanceof DatabaseStartAbortedException;
    }

    private Stream<Transition.Prepared> getLifecycleTransitionSteps( EnterpriseDatabaseState currentState, EnterpriseDatabaseState desiredState )
    {
        return transitionsTable.fromCurrent( currentState ).toDesired( desiredState );
    }

    public final void registerDatabaseStateChangedListener( DatabaseStateChangedListener listener )
    {
        listeners.add( listener );
    }
}
