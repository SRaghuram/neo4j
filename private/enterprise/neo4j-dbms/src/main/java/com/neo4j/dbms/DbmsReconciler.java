/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.MultiDatabaseManager;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.function.Function;
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
import static com.neo4j.dbms.OperatorState.STARTED;
import static com.neo4j.dbms.OperatorState.STOPPED;
import static java.lang.String.format;
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
    private final Set<DatabaseId> reconciling;
    private final MultiDatabaseManager<? extends DatabaseContext> databaseManager;
    private final BinaryOperator<OperatorState> precedence;
    private final Log log;
    private final boolean canRetry;
    private final Map<DatabaseId,DatabaseReconcilerState> currentStates;

    DbmsReconciler( MultiDatabaseManager<? extends DatabaseContext> databaseManager, Config config, LogProvider logProvider, JobScheduler scheduler )
    {
        this.databaseManager = databaseManager;

        this.backoffStrategy = new ExponentialBackoffStrategy(
                config.get( GraphDatabaseSettings.reconciler_minimum_backoff ),
                config.get( GraphDatabaseSettings.reconciler_maximum_backoff ) );
        this.executor = scheduler.executor( Group.DATABASE_RECONCILER );
        this.reconciling = ConcurrentHashMap.newKeySet();
        this.currentStates = new ConcurrentHashMap<>();
        this.log = logProvider.getLog( getClass() );
        this.canRetry = config.get( GraphDatabaseSettings.reconciler_may_retry );
        this.precedence = OperatorState::minByPrecedence;
    }

    Reconciliation reconcile( List<DbmsOperator> operators, ReconcilerRequest request )
    {
        var dbsToReconcile = operators.stream()
                .flatMap( op -> op.desired().keySet().stream() )
                .distinct();

        var reconciliation = dbsToReconcile
                .map( dbId -> Pair.of( dbId, reconcile( dbId, request, operators ) ) )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );

        return new Reconciliation( reconciliation );
    }

    private static Map<DatabaseId,OperatorState> combineDesiredStates( Map<DatabaseId,OperatorState> combined, Map<DatabaseId,OperatorState> operator,
            BinaryOperator<OperatorState> precedence )
    {
        return Stream.concat( combined.entrySet().stream(), operator.entrySet().stream() )
                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue, precedence ) );
    }

    private static Map<DatabaseId,OperatorState> desiredStates( List<DbmsOperator> operators, BinaryOperator<OperatorState> precedence )
    {
        return operators.stream()
                .map( DbmsOperator::desired )
                .reduce( new HashMap<>(), ( l, r ) -> DbmsReconciler.combineDesiredStates( l, r, precedence ) );
    }

    private CompletableFuture<ReconcilerStepResult> reconcile( DatabaseId databaseId, ReconcilerRequest request, List<DbmsOperator> operators )
    {
        var work = preReconcile( databaseId, operators )
                .thenCompose( desiredState ->
                {
                    var reconcilerEntry = currentStates.getOrDefault( databaseId, DatabaseReconcilerState.INITIAL );
                    var initialResult = new ReconcilerStepResult( reconcilerEntry.state(), null, desiredState );

                    if ( reconcilerEntry.state() == desiredState )
                    {
                        return CompletableFuture.completedFuture( initialResult );
                    }

                    if ( reconcilerEntry.hasFailed() && !request.forceReconciliation() )
                    {
                        var message = format( "Attempting to reconcile database %s to state '%s' but has previously failed. Manual force is required to retry.",
                                databaseId.name(), desiredState.description() );
                        return CompletableFuture.completedFuture( initialResult.withError( new DatabaseManagementException( message ) ) );
                    }

                    var backoff = backoffStrategy.newTimeout();
                    var steps = prepareLifeCycleTransitionSteps( reconcilerEntry.state(), databaseId, desiredState );

                    return CompletableFuture.supplyAsync( () -> reconcileSteps( initialResult.state(), steps, databaseId, desiredState ), executor  )
                            .thenCompose( result -> retry( databaseId, desiredState, result, executor, backoff, 0 ) );
                } );

        return postReconcile( work, databaseId, request );
    }

    private CompletableFuture<ReconcilerStepResult> retry( DatabaseId databaseId, OperatorState desiredState, ReconcilerStepResult result, Executor executor,
            TimeoutStrategy.Timeout backoff, int retries )
    {
        boolean isFatalError = result.error() != null && !isTransientError( result.error() );
        if ( result.error() == null || isFatalError )
        {
            return CompletableFuture.completedFuture( result );
        }

        var attempt = retries + 1;
        log.warn( "Retrying reconciliation of database %s to state '%s'. This is attempt %d.", databaseId.name(), desiredState.description(), attempt );

        var remainingSteps = prepareLifeCycleTransitionSteps( result.state(), databaseId, desiredState );
        return CompletableFuture.supplyAsync( () -> reconcileSteps( result.state(), remainingSteps, databaseId, desiredState ),
                delayedExecutor( backoff.getAndIncrement(), TimeUnit.MILLISECONDS, executor ) )
                .thenCompose( retryResult -> retry( databaseId, desiredState, retryResult, executor, backoff, attempt ) );
    }

    private CompletableFuture<OperatorState> preReconcile( DatabaseId databaseId, List<DbmsOperator> operators )
    {
        return CompletableFuture.supplyAsync( () ->
        {
            try
            {
                acquireLockOn( databaseId );

                var desiredState = desiredStates( operators, precedence ).get( databaseId );
                if ( desiredState == null )
                {
                    var cause = new NullPointerException( format( "No operator desires a state for database %s any more. " +
                            "This is likely an error!", databaseId ) );
                    //TODO: investigate whether we need to wrap
                    throw new CompletionException( cause );
                }
                return desiredState;
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                throw new CompletionException( e );
            }

        }, executor );
    }

    private CompletableFuture<ReconcilerStepResult> postReconcile( CompletableFuture<ReconcilerStepResult> work, DatabaseId databaseId,
            ReconcilerRequest request )
    {
        return work.whenComplete( ( result, throwable ) ->
        {
            currentStates.compute( databaseId, ( id, entry ) ->
            {
                if ( throwable != null )
                {
                    log.error( format( "Encountered unexpected error when attempting to reconcile database %s", databaseId.name() ), throwable );
                    return entry == null ? DatabaseReconcilerState.INITIAL.failed() : entry.failed();
                }
                else if ( result.error() != null )
                {
                    log.error( format( "Encountered error when attempting to reconcile database %s from state '%s' to state '%s'",
                            databaseId.name(), result.state(), result.desiredState().description() ), result.error() );
                    return new DatabaseReconcilerState( result.state() ).failed();
                }
                //TODO: Should we panic the database in both failure cases above? In the case of certain errors (OOM) should we panic the whole DBMS?

                var reconcilerState = new DatabaseReconcilerState( result.state() );
                if ( shouldFailDatabaseStateAfterSuccessfulReconcile( databaseId, entry, request ) )
                {
                    return reconcilerState.failed();
                }
                return reconcilerState;
            } );
            releaseLockOn( databaseId );
        } );
    }

    private static boolean shouldFailDatabaseStateAfterSuccessfulReconcile( DatabaseId databaseId, DatabaseReconcilerState currentState,
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
        return request.shouldFailAfterReconciliation( databaseId );
    }

    private void releaseLockOn( DatabaseId databaseId )
    {
        synchronized ( reconciling )
        {
            reconciling.remove( databaseId );
            reconciling.notifyAll();
        }
    }

    private void acquireLockOn( DatabaseId databaseId ) throws InterruptedException
    {
        synchronized ( reconciling )
        {
            while ( reconciling.contains( databaseId ) )
            {
                reconciling.wait();
            }
            reconciling.add( databaseId );
        }
    }

    private boolean isTransientError( Throwable t )
    {
        return canRetry && !( t instanceof Error );
    }

    private ReconcilerStepResult reconcileSteps( OperatorState currentState, Stream<Function<DatabaseId,OperatorState>> steps, DatabaseId databaseId,
            OperatorState desiredState )
    {
        return reconcileSteps0( steps.iterator(), new ReconcilerStepResult( currentState, null, desiredState ), databaseId );
    }

    private ReconcilerStepResult reconcileSteps0( Iterator<Function<DatabaseId,OperatorState>> steps, ReconcilerStepResult result, DatabaseId databaseId )
    {
        if ( !steps.hasNext() )
        {
            return result;
        }

        try
        {
            var nextState = steps.next().apply( databaseId );
            return reconcileSteps0( steps, result.withState( nextState ), databaseId );
        }
        catch ( DatabaseManagementException e )
        {
            return result.withError( e );
        }
        catch ( Exception e )
        {
            System.out.println("Hola");
            return null;
        }
    }

    protected Stream<Function<DatabaseId,OperatorState>> prepareLifeCycleTransitionSteps( OperatorState currentState, DatabaseId databaseId,
            OperatorState desiredState )
    {
        if ( currentState == desiredState || ( currentState == null && desiredState == DROPPED ) )
        {
            //If the current and desired state are the same, or if current state is null and desired is DROPPED,
            // then there are no lifecycle transitions to be performed
            return Stream.of( ignored -> desiredState );
        }
        else if ( currentState == null )
        {
            return Stream.concat( Stream.of( this::create ), prepareLifeCycleTransitionSteps( STOPPED, databaseId, desiredState ) );
        }
        else if ( currentState == STOPPED && desiredState == STARTED )
        {
            return Stream.of( this::start );
        }
        else if ( currentState == STARTED && desiredState == STOPPED )
        {
            return Stream.of( this::stop );
        }
        else if ( currentState == STOPPED && desiredState == DROPPED )
        {
            return Stream.of( this::drop );
        }
        else if ( currentState == STARTED && desiredState == DROPPED )
        {
            return Stream.of( this::prepareDrop, this::stop, this::drop );
        }
        else if ( currentState == DROPPED )
        {
            throw new IllegalArgumentException( format( "Trying to set database %s to %s, but is 'dropped'. 'dropped' is a final state for databases!",
                    databaseId.name(), desiredState.description() ) );
        }

        throw new IllegalArgumentException( format( "'%s' -> '%s' is an unsupported state transition for the database %s!", currentState.description(),
                desiredState.description(), databaseId.name() ) );
    }

    /* Operator Steps */
    protected final OperatorState stop( DatabaseId databaseId )
    {
        databaseManager.stopDatabase( databaseId );
        return STOPPED;
    }

    protected final OperatorState prepareDrop( DatabaseId databaseId )
    {
        databaseManager.getDatabaseContext( databaseId )
                .map( DatabaseContext::database )
                .ifPresent( Database::prepareToDrop );
        return STARTED;
    }

    protected final OperatorState drop( DatabaseId databaseId )
    {
        databaseManager.dropDatabase( databaseId );
        return DROPPED;
    }

    protected final OperatorState start( DatabaseId databaseId )
    {
        databaseManager.startDatabase( databaseId );
        return STARTED;
    }

    protected final OperatorState create( DatabaseId databaseId )
    {
        databaseManager.createDatabase( databaseId, false );
        return STOPPED;
    }
}
