/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.Transitions.Transition;
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
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.helpers.ExponentialBackoffStrategy;
import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static com.neo4j.dbms.OperatorState.DROPPED;
import static com.neo4j.dbms.OperatorState.INITIAL;
import static com.neo4j.dbms.OperatorState.STARTED;
import static com.neo4j.dbms.OperatorState.STOPPED;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.CompletableFuture.delayedExecutor;

/**
 * Responsible for controlling the lifecycles of *all* databases in this neo4j instance, via the {@link DatabaseManager}.
 * As opposed to the imperative interface provided by a DatabaseManager, the reconciler offers a declarative interface
 * whereby a user, or internal system components, declare the state (see {@link OperatorState}) they *desire* a database
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
 * instances. When triggered, the reconciler fetches the desired states from each operator as {@code Map<DatabaseId,OperatorState>}
 * and merges them. When multiple operators specify a desired state for the same database, one state is chosen according to the
 * {@code this.precedence} binary operator. The final merged map of DatabaseIds to OperatorStates is compared to {this.currentStates}
 * and changes are made where necessary.
 *
 * Note: With the exception of short lived instances, sometimes created as a side effect of creating a database, reconcilers are global singletons.
 */
public class DbmsReconciler implements DatabaseStateService
{
    private final ExponentialBackoffStrategy backoffStrategy;
    private final Executor executor;
    private final Map<String,CompletableFuture<ReconcilerStepResult>> reconcilerJobCache;
    private final Set<String> reconciling;
    private final MultiDatabaseManager<? extends DatabaseContext> databaseManager;
    private final BinaryOperator<DatabaseState> precedence;
    private final Log log;
    private final boolean canRetry;
    protected final Map<String,DatabaseState> currentStates;
    private final Transitions transitions;

    DbmsReconciler( MultiDatabaseManager<? extends DatabaseContext> databaseManager, Config config, LogProvider logProvider, JobScheduler scheduler )
    {
        this.databaseManager = databaseManager;

        this.canRetry = config.get( GraphDatabaseSettings.reconciler_may_retry );
        this.backoffStrategy = new ExponentialBackoffStrategy(
                config.get( GraphDatabaseSettings.reconciler_minimum_backoff ),
                config.get( GraphDatabaseSettings.reconciler_maximum_backoff ) );

        if ( config.isExplicitlySet( GraphDatabaseSettings.reconciler_maximum_parallelism ) )
        {
            int parallelism = config.get( GraphDatabaseSettings.reconciler_maximum_parallelism );
            parallelism = parallelism == 0 ? Runtime.getRuntime().availableProcessors() : parallelism;
            scheduler.setParallelism( Group.DATABASE_RECONCILER , parallelism );
        }
        this.executor = scheduler.executor( Group.DATABASE_RECONCILER );

        this.reconciling = new HashSet<>();
        this.currentStates = new ConcurrentHashMap<>();
        this.reconcilerJobCache = new ConcurrentHashMap<>();
        this.log = logProvider.getLog( getClass() );
        this.precedence = OperatorState.minByOperatorState( DatabaseState::operationalState );
        this.transitions = prepareLifecycleTransitionSteps();
    }

    @Override
    public String stateOfDatabase( DatabaseId databaseId )
    {
        return currentStates.getOrDefault( databaseId.name(), DatabaseState.unknown( databaseId ) ).operationalState().description();
    }

    @Override
    public Optional<Throwable> causeOfFailure( DatabaseId databaseId )
    {
        return currentStates.getOrDefault( databaseId.name(), DatabaseState.unknown( databaseId ) ).failure();
    }

    ReconcilerResult reconcile( List<DbmsOperator> operators, ReconcilerRequest request )
    {
        var namesOfDbsToReconcile = operators.stream()
                .flatMap( op -> op.desired().keySet().stream() )
                .distinct();

        var reconciliation = namesOfDbsToReconcile
                .map( dbName -> Pair.of( dbName, reconcile( dbName, request, operators ) ) )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );

        return new ReconcilerResult( reconciliation );
    }

    private static Map<String,DatabaseState> combineDesiredStates( Map<String,DatabaseState> combined, Map<String,DatabaseState> operator,
            BinaryOperator<DatabaseState> precedence )
    {
        return Stream.concat( combined.entrySet().stream(), operator.entrySet().stream() )
                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue, precedence ) );
    }

    private static Map<String,DatabaseState> desiredStates( List<DbmsOperator> operators, BinaryOperator<DatabaseState> precedence )
    {
        return operators.stream()
                .map( DbmsOperator::desired )
                .reduce( new HashMap<>(), ( l, r ) -> DbmsReconciler.combineDesiredStates( l, r, precedence ) );
    }

    protected DatabaseState getReconcilerEntryFor( DatabaseId databaseId )
    {
        return currentStates.getOrDefault( databaseId.name(), DatabaseState.initial( databaseId ) );
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
    private synchronized CompletableFuture<ReconcilerStepResult> reconcile( String databaseName, ReconcilerRequest request, List<DbmsOperator> operators )
    {
        var cachedJob = reconcilerJobCache.get( databaseName );
        if ( cachedJob != null && request.isSimple() )
        {
            return cachedJob;
        }

        var reconcilerJobHandle = new CompletableFuture<Void>();

        var job = reconcilerJobHandle
                .thenCompose( ignored -> preReconcile( databaseName, operators, request ) )
                .thenCompose( desiredState -> reconcileRetry( databaseName, desiredState, request ) )
                .whenComplete( ( result, throwable ) -> postReconcile( databaseName, request, result, throwable ) );

        if ( request.isSimple() )
        {
            reconcilerJobCache.put( databaseName, job );
        }
        //Having synchronously placed the job future in the cache (and thus avoiding potential races)
        // we can now start the job by completing the Void handle future at the head of the chain.
        reconcilerJobHandle.complete( null );
        return job;
    }

    private CompletableFuture<DatabaseState> preReconcile( String databaseName, List<DbmsOperator> operators, ReconcilerRequest request )
    {
        return CompletableFuture.supplyAsync( () ->
        {
            try
            {
                log.debug( "Attempting to acquire lock before reconciling state of database `%s`.", databaseName );
                acquireLockOn( databaseName );

                if ( request.isSimple() )
                {
                    reconcilerJobCache.remove( databaseName );
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

        }, executor );
    }

    private CompletableFuture<ReconcilerStepResult> reconcileRetry( String databaseName, DatabaseState desiredState, ReconcilerRequest request )
    {
        var currentState = getReconcilerEntryFor( desiredState.databaseId() );

        var initialResult = new ReconcilerStepResult( currentState, null, desiredState );

        if ( currentState.equals( desiredState ) )
        {
            return CompletableFuture.completedFuture( initialResult );
        }

        if ( currentState.hasFailed() && !request.forceReconciliation() )
        {
            var message = format( "Attempting to reconcile database %s to state '%s' but has previously failed. Manual force is required to retry.",
                    databaseName, desiredState.operationalState().description() );
            return CompletableFuture.completedFuture( initialResult.withError( new DatabaseManagementException( message ) ) );
        }

        var backoff = backoffStrategy.newTimeout();
        var steps = getLifecycleTransitionSteps( currentState, desiredState );

        DatabaseId databaseId = desiredState.databaseId();
        return CompletableFuture.supplyAsync( () -> reconcileSteps( initialResult.state(), steps, desiredState ), executor  )
                .thenCompose( result -> retry( databaseId, desiredState, result, executor, backoff, 0 ) );
    }

    private static ReconcilerStepResult reconcileSteps( DatabaseState currentState, Stream<Transition> steps, DatabaseState desiredState )
    {
        String oldThreadName = currentThread().getName();
        try
        {
            currentThread().setName( oldThreadName + "-" + desiredState.databaseId().name() );
            return reconcileSteps0( steps.iterator(), new ReconcilerStepResult( currentState, null, desiredState ) );
        }
        finally
        {
            currentThread().setName( oldThreadName );
        }
    }

    private static ReconcilerStepResult reconcileSteps0( Iterator<Transition> steps, ReconcilerStepResult result )
    {
        if ( !steps.hasNext() )
        {
            return result;
        }

        try
        {
            var nextState = steps.next().doTransition();
            return reconcileSteps0( steps, result.withState( nextState ) );
        }
        catch ( DatabaseManagementException e )
        {
            return result.withError( e );
        }
    }

    private CompletableFuture<ReconcilerStepResult> retry( DatabaseId databaseId, DatabaseState desiredState, ReconcilerStepResult result, Executor executor,
            TimeoutStrategy.Timeout backoff, int retries )
    {
        boolean isFatalError = result.error() != null && !isTransientError( result.error() );
        if ( result.error() == null || isFatalError )
        {
            return CompletableFuture.completedFuture( result );
        }

        var attempt = retries + 1;
        log.warn( "Retrying reconciliation of database %s to state '%s'. This is attempt %d.", databaseId.name(),
                desiredState.operationalState().description(), attempt );

        var remainingSteps = getLifecycleTransitionSteps( result.state(), desiredState );
        return CompletableFuture.supplyAsync( () -> reconcileSteps( result.state(), remainingSteps, desiredState ),
                delayedExecutor( backoff.getAndIncrement(), TimeUnit.MILLISECONDS, executor ) )
                .thenCompose( retryResult -> retry( databaseId, desiredState, retryResult, executor, backoff, attempt ) );
    }

    private void postReconcile( String databaseName, ReconcilerRequest request, ReconcilerStepResult result, Throwable throwable )
    {
        try
        {
            currentStates.compute( databaseName, ( name, previousState ) ->
            {
                if ( throwable != null )
                {
                    var message = format( "Encountered unexpected error when attempting to reconcile database %s", databaseName );
                    if ( previousState == null )
                    {
                        log.error( message, throwable );
                        return DatabaseState.initial( null ).failed( throwable );
                    }
                    else
                    {
                        reportErrorAndPanicDatabase( previousState.databaseId(), message, throwable );
                        return previousState.failed( throwable );
                    }
                }
                else if ( result.error() != null )
                {
                    var message = format( "Encountered error when attempting to reconcile database %s from state '%s' to state '%s'",
                            databaseName, result.state(), result.desiredState().operationalState().description() );
                    reportErrorAndPanicDatabase( result.state().databaseId(), message, result.error() );
                    return result.state().failed( result.error() );
                }

                var nextState = result.state();
                var failure = shouldFailDatabaseWithCausePostSuccessfulReconcile( nextState.databaseId(), previousState, request );
                return failure.map( nextState::failed ).orElse( nextState );
            } );
        }
        finally
        {
            releaseLockOn( databaseName );
            var errorExists = throwable != null || result.error() != null;
            var outcome = errorExists ? "failed" : "succeeded";
            log.debug( "Released lock having %s to reconcile database `%s` to state %s.", outcome, databaseName,
                    result.desiredState().operationalState().description() );
        }
    }

    private void reportErrorAndPanicDatabase( DatabaseId databaseId, String message, Throwable error )
    {
        log.error( message, error );
        var panicCause = new IllegalStateException( message, error );
        panicDatabase( databaseId, panicCause );
    }

    private static Optional<Throwable> shouldFailDatabaseWithCausePostSuccessfulReconcile( DatabaseId databaseId, DatabaseState currentState,
            ReconcilerRequest request )
    {
        if ( request.forceReconciliation() )
        {
            // a successful reconcile operation was forced, no need to keep the failed state
            return Optional.empty();
        }
        if ( currentState != null && currentState.hasFailed() )
        {
            // preserve the current failed state because we didn't force reconciliation
            return currentState.failure();
        }
        return request.causeOfPanic( databaseId );
    }

    private void releaseLockOn( String databaseName )
    {
        synchronized ( reconciling )
        {
            reconciling.remove( databaseName );
            reconciling.notifyAll();
        }
    }

    private void acquireLockOn( String databaseName ) throws InterruptedException
    {
        synchronized ( reconciling )
        {
            while ( reconciling.contains( databaseName ) )
            {
                reconciling.wait();
            }
            reconciling.add( databaseName );
        }
    }

    private boolean isTransientError( Throwable t )
    {
        return canRetry && !( t instanceof Error );
    }

    /**
     * This method defines the table mapping any pair of database states to the series of steps the reconciler needs to perform
     * to take a database from one state to another.
     */
    protected Transitions prepareLifecycleTransitionSteps()
    {
        return Transitions.builder()
                .from( INITIAL ).to( DROPPED ).doNothing()
                .from( INITIAL ).to( STOPPED ).doTransitions( this::create )
                .from( INITIAL ).to( STARTED ).doTransitions( this::create, this::start )
                .from( STOPPED ).to( STARTED ).doTransitions( this::start )
                .from( STARTED ).to( STOPPED ).doTransitions( this::stop )
                .from( STOPPED ).to( DROPPED ).doTransitions( this::drop )
                .from( STARTED ).to( DROPPED ).doTransitions( this::prepareDrop, this::stop, this::drop )
                .build();
    }

    private Stream<Transition> getLifecycleTransitionSteps( DatabaseState currentState, DatabaseState desiredState )
    {
        return transitions.fromCurrent( currentState ).toDesired( desiredState );
    }

    protected void panicDatabase( DatabaseId databaseId, Throwable error )
    {
        databaseManager.getDatabaseContext( databaseId )
                .map( ctx -> ctx.database().getDatabaseHealth() )
                .ifPresent( health -> health.panic( error ) );
    }

    /* Operator Steps */
    protected final DatabaseState stop( DatabaseId databaseId )
    {
        databaseManager.stopDatabase( databaseId );
        return new DatabaseState( databaseId, STOPPED );
    }

    private DatabaseState prepareDrop( DatabaseId databaseId )
    {
        databaseManager.getDatabaseContext( databaseId )
                .map( DatabaseContext::database )
                .ifPresent( Database::prepareToDrop );
        return new DatabaseState( databaseId, STARTED );
    }

    protected final DatabaseState drop( DatabaseId databaseId )
    {
        databaseManager.dropDatabase( databaseId );
        return new DatabaseState( databaseId, DROPPED );
    }

    protected final DatabaseState start( DatabaseId databaseId )
    {
        databaseManager.startDatabase( databaseId );
        return new DatabaseState( databaseId, STARTED );
    }

    protected final DatabaseState create( DatabaseId databaseId )
    {
        databaseManager.createDatabase( databaseId );
        return new DatabaseState( databaseId, STOPPED );
    }
}
