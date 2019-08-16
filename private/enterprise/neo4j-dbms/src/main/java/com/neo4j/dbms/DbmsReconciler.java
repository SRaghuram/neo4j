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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
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
public class DbmsReconciler
{
    private final ExponentialBackoffStrategy backoffStrategy;
    private final Executor executor;
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
        this.backoffStrategy = new ExponentialBackoffStrategy(
                config.get( GraphDatabaseSettings.reconciler_minimum_backoff ),
                config.get( GraphDatabaseSettings.reconciler_maximum_backoff ) );

        int parallelism = config.get( GraphDatabaseSettings.reconciler_maximum_parallelism );
        parallelism = parallelism == 0 ? Runtime.getRuntime().availableProcessors() : parallelism;
        scheduler.setParallelism( Group.DATABASE_RECONCILER , parallelism );
        this.executor = scheduler.executor( Group.DATABASE_RECONCILER );

        this.reconciling = new HashSet<>();
        this.currentStates = new ConcurrentHashMap<>();
        this.log = logProvider.getLog( getClass() );
        this.canRetry = config.get( GraphDatabaseSettings.reconciler_may_retry );
        this.precedence = OperatorState.minByOperatorState( DatabaseState::operationalState );
        this.transitions = prepareLifecycleTransitionSteps();
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

    protected DatabaseState getReconcilerEntryFor( DatabaseId databaseId )
    {
        return currentStates.getOrDefault( databaseId.name(), DatabaseState.initial( databaseId ) );
    }

    private CompletableFuture<ReconcilerStepResult> reconcile( String databaseName, ReconcilerRequest request, List<DbmsOperator> operators )
    {
        var work = preReconcile( databaseName, operators )
                .thenCompose( desiredState ->
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
                } );

        return postReconcile( work, databaseName, request );
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

    private CompletableFuture<DatabaseState> preReconcile( String databaseName, List<DbmsOperator> operators )
    {
        return CompletableFuture.supplyAsync( () ->
        {
            try
            {
                acquireLockOn( databaseName );

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

    private CompletableFuture<ReconcilerStepResult> postReconcile( CompletableFuture<ReconcilerStepResult> work, String databaseName,
            ReconcilerRequest request )
    {
        return work.whenComplete( ( result, throwable ) ->
        {
            try
            {
                currentStates.compute( databaseName, ( name, entry ) ->
                {
                    if ( throwable != null )
                    {
                        var message = format( "Encountered unexpected error when attempting to reconcile database %s", databaseName );
                        if ( entry == null )
                        {
                            log.error( message, throwable );
                            return DatabaseState.initial( null ).failed();
                        }
                        else
                        {
                            reportErrorAndPanicDatabase( entry.databaseId(), message, throwable );
                            return entry.failed();
                        }
                    }
                    else if ( result.error() != null )
                    {
                        var message = format( "Encountered error when attempting to reconcile database %s from state '%s' to state '%s'",
                                databaseName, result.state(), result.desiredState().operationalState().description() );
                        reportErrorAndPanicDatabase( result.state().databaseId(), message, result.error() );
                        return result.state().failed();
                    }

                    var reconcilerState = result.state();
                    if ( shouldFailDatabaseStateAfterSuccessfulReconcile( result.state().databaseId(), entry, request ) )
                    {
                        return reconcilerState.failed();
                    }
                    return reconcilerState;
                } );
            }
            finally
            {
                releaseLockOn( databaseName );
            }
        } );
    }

    private void reportErrorAndPanicDatabase( DatabaseId databaseId, String message, Throwable error )
    {
        log.error( message, error );
        var panicCause = new IllegalStateException( message, error );
        panicDatabase( databaseId, panicCause );
    }

    private static boolean shouldFailDatabaseStateAfterSuccessfulReconcile( DatabaseId databaseId, DatabaseState currentState,
            ReconcilerRequest request )
    {
        if ( request.forceReconciliation() )
        {
            // a successful reconcile operation was forced, no need to keep the failed state
            return false;
        }
        if ( currentState != null && currentState.hasFailed() )
        {
            // preserve the current failed state
            return true;
        }
        return request.databasePanicked( databaseId );
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
