/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload.full;

import com.neo4j.cc_robustness.CcInstance;
import com.neo4j.cc_robustness.CcRobustness;
import com.neo4j.cc_robustness.Orchestrator;
import com.neo4j.cc_robustness.util.CommandReactor;
import com.neo4j.cc_robustness.util.Duration;
import com.neo4j.cc_robustness.workload.GraphOperations.Operation;
import com.neo4j.cc_robustness.workload.InstanceSelector;
import com.neo4j.cc_robustness.workload.InstanceSelectors;
import com.neo4j.cc_robustness.workload.SchemaOperation;
import com.neo4j.cc_robustness.workload.SchemaOperationType;
import com.neo4j.cc_robustness.workload.SchemaOperations;
import com.neo4j.cc_robustness.workload.ShutdownType;
import com.neo4j.cc_robustness.workload.ShutdownTypeSelector;
import com.neo4j.cc_robustness.workload.WorkLoad;
import com.neo4j.configuration.CausalClusteringSettings;
import org.apache.lucene.util.NamedThreadFactory;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.function.ThrowingAction;
import org.neo4j.internal.helpers.Args;
import org.neo4j.internal.helpers.Listeners;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.cc_robustness.CcRobustness.MYNOCK_MEAN_OUTAGE_TIME;
import static com.neo4j.cc_robustness.CcRobustness.ROTATE;
import static com.neo4j.cc_robustness.util.TimeUtil.parseDuration;
import static com.neo4j.cc_robustness.workload.SchemaOperationType.indexing;
import static com.neo4j.cc_robustness.workload.SchemaOperationType.nodePropertyExistenceConstraints;
import static com.neo4j.cc_robustness.workload.SchemaOperationType.relationshipPropertyExistenceConstraints;
import static com.neo4j.cc_robustness.workload.SchemaOperationType.uniquenessConstraints;
import static java.lang.Math.abs;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.function.Predicates.notNull;
import static org.neo4j.internal.helpers.TimeUtil.parseTimeMillis;
import static org.neo4j.internal.helpers.collection.Iterables.addAll;
import static org.neo4j.internal.helpers.collection.Iterables.iterable;

public class FullOnWorkLoad implements WorkLoad
{
    private static final AtomicInteger WORKER_ID = new AtomicInteger();

    private final int numberOfWorkers;
    private final int txSize;
    private final List<Worker> workers = new ArrayList<>();
    private final int secondsToRun;
    private final ScheduledExecutorService statsExecutor = new ScheduledThreadPoolExecutor( 1, new NamedThreadFactory( "CC robustness health check" ) );
    private final EnumMap<TransactionOutcome,OperationCount> counts = new EnumMap<>( TransactionOutcome.class );
    private final Map<Integer,AtomicInteger> consecutiveExceptions = new HashMap<>();
    private final Map<Integer,List<String>> consecutiveExceptionsList = new HashMap<>();
    private final AtomicBoolean pause = new AtomicBoolean();
    private final AtomicBoolean pauseKiller = new AtomicBoolean();
    private final InstanceSelector writeSelector;
    private final InstanceSelector killSelector;
    private final ShutdownTypeSelector shutdownTypeSelector;
    private final Duration rotationInterval;
    private final Gentleness gentleness;
    private final int initialSize;
    private final int killInterval;
    private final Duration resurrectWait;
    private final int opInterval;
    private final float bigTransactionProbability;
    private final Duration waitAfterFailure;
    private final RemoteControl remoteControl = new FullRemoteControl();
    private final ReportingFactory reportingFactory = new FullReportingFactory();
    private final InstanceHealth instanceHealth = new FullInstanceHealth();
    private final int maxClusterUnresponsiveTimeAllowed;
    private final Collection<SchemaOperationType> schemaOperations;
    private final int schemaOperationsInterval;
    private final int mynockCount;
    private final Args additionalArgs;
    private final InstanceSelector mynockSelector;
    private final List<Worker> mynocks = new ArrayList<>();
    private final Duration mynockInterval;
    private Killer killer;
    private LeaderChecker leaderChecker;
    private RotatorWorker rotator;
    private SchemaWorker schemaWorker;
    private volatile boolean globalHalt;
    private long startTime;
    private volatile Throwable fatalError;
    private Orchestrator orchestrator;
    private LogProvider logProvider;
    private Log log;
    private Duration mynockMeanOutage;

    public FullOnWorkLoad( int secondsToRun, int numberOfWorkers, int txSize, int clusterSize, InstanceSelector killSelector,
            ShutdownTypeSelector shutdownTypeSelector, Args additionalArgs, InstanceSelector writeSelector )
    {
        this.secondsToRun = secondsToRun;
        this.numberOfWorkers = numberOfWorkers;
        this.txSize = txSize;
        this.additionalArgs = additionalArgs;
        this.writeSelector = writeSelector;
        this.killSelector = killSelector;
        this.shutdownTypeSelector = shutdownTypeSelector;
        this.rotationInterval = additionalArgs.has( ROTATE ) ? parseDuration( additionalArgs.get( ROTATE, null ) ) : null;
        this.gentleness = additionalArgs.getEnum( Gentleness.class, "gentleness", Gentleness.not );
        this.initialSize = additionalArgs.getNumber( "initial-size", 10000 ).intValue();
        this.killInterval = parseTimeMillis.apply( additionalArgs.get( "kill-interval", "30s" ) ).intValue();
        this.resurrectWait = parseDuration( additionalArgs.get( "resurrect-wait", killInterval + "ms" ), 0.1 );
        this.opInterval = parseTimeMillis.apply( additionalArgs.get( "op-interval", "0" ) ).intValue();
        this.bigTransactionProbability = additionalArgs.getNumber( "big-transaction-probability", 0.002f ).floatValue();
        this.maxClusterUnresponsiveTimeAllowed = additionalArgs.getNumber( "cluster-unresponsive-timeout-seconds", 300 ).intValue();
        this.schemaOperations = schemaOperations( additionalArgs.get(
                "schema-operations", join(
                        ",", iterable( indexing.name(), uniquenessConstraints.name(), nodePropertyExistenceConstraints.name(),
                                relationshipPropertyExistenceConstraints.name() ) ) ) );
        this.schemaOperationsInterval = parseTimeMillis.apply( additionalArgs.get( "schema-operations-interval", "180s" ) ).intValue();
        this.mynockCount = Integer.parseInt( additionalArgs.get( CcRobustness.MYNOCKS, "" + CcRobustness.DEFAULT_MYNOCKS ) );
        this.mynockSelector = InstanceSelectors.selector( additionalArgs.get( CcRobustness.MYNOCK_SELECTOR, CcRobustness.DEFAULT_MYNOCK_SELECTOR.name() ) );
        this.mynockInterval = parseDuration( additionalArgs.get( CcRobustness.MYNOCK_INTERVAL, CcRobustness.DEFAULT_MYNOCK_INTERVAL ), 0.5d );

        for ( TransactionOutcome type : TransactionOutcome.values() )
        {
            counts.put( type, new OperationCount() );
        }
        for ( int i = 0; i < clusterSize; i++ )
        {
            consecutiveExceptions.put( i, new AtomicInteger() );
            consecutiveExceptionsList.put( i, new ArrayList<>() );
        }
        waitAfterFailure = new Duration( limit( 2 ), SECONDS );
    }

    private Collection<SchemaOperationType> schemaOperations( String arg )
    {
        if ( arg == null || arg.length() == 0 )
        {
            return emptyList();
        }

        Collection<SchemaOperationType> result = new ArrayList<>();
        for ( String token : arg.split( "," ) )
        {
            result.add( SchemaOperationType.valueOf( token ) );
        }
        return result;
    }

    @Override
    public void perform( Orchestrator orchestrator, LogProvider logProvider ) throws Exception
    {
        // Get outage time from user specified config, or calculate based on other configured timeouts
        double variance = 0.6;
        this.mynockMeanOutage = additionalArgs.has( MYNOCK_MEAN_OUTAGE_TIME ) ? parseDuration( additionalArgs.get( MYNOCK_MEAN_OUTAGE_TIME ), variance )
                                                                              : new Duration( orchestrator
                                                                                      .getDbConfig(
                                                                                              CausalClusteringSettings.catch_up_client_inactivity_timeout )
                                                                                      .toMillis(), MILLISECONDS, variance );

        this.orchestrator = orchestrator;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
        createInitialDataset( orchestrator );
        startTime = currentTimeMillis();
        Condition endCondition = new Condition()
        {
            private final Condition endTimeCondition = new TimedCondition( new Duration( secondsToRun, SECONDS ) );

            @Override
            public boolean met()
            {
                if ( endTimeCondition.met() || globalHalt || fatalError != null )
                {
                    log.info( "[" + Thread.currentThread().getName() + "] End condition met due to:" );
                    if ( endTimeCondition.met() )
                    {
                        log.info( "  end time reached" );
                    }
                    if ( globalHalt )
                    {
                        log.info( "  globalHalt" );
                    }
                    if ( fatalError != null )
                    {
                        log.info( "  fatalError", fatalError );
                    }
                    return true;
                }
                return false;
            }
        };

        slowDownLogging( additionalArgs.get( CcRobustness.SLOW_LOGGING_PROBABILITY, CcRobustness.DEFAULT_SLOW_LOGGING_PROBABILITY ) );
        startWorkers( endCondition );
        startKiller( endCondition );
        startLeaderChecker( endCondition );
        startRotator( endCondition );
        startSchemaWorker( endCondition );
        startMynocks( endCondition );
        startHealthCheck( endCondition );

        log.info( "All " + workers.size() + " workers are running" );
        CommandReactor reactor = startReactingToUserInput();
        killer.join();
        Throwable fatalError = waitForWorkersToExit();
        reactor.shutdown();
        statsExecutor.shutdown();
        if ( fatalError != null )
        {
            throw new RuntimeException( fatalError );
        }
    }

    private void slowDownLogging( String slowdownProbability )
    {
        float probability;
        try
        {
            probability = Float.parseFloat( slowdownProbability );
        }
        catch ( NumberFormatException e )
        {
            probability = 0;
        }
        for ( CcInstance instance : orchestrator.getAliveCcInstances() )
        {
            try
            {
                instance.slowDownLogging( probability );
            }
            catch ( RemoteException e )
            {
                e.printStackTrace();
            }
        }
    }

    private void startMynocks( Condition endCondition )
    {
        if ( mynockCount > 0 )
        {
            log.info( "Releasing " + mynockCount + " mynock" + (mynockCount > 1 ? "s" : "") );
        }

        for ( int i = 0; i < mynockCount; i++ )
        {
            Mynock.Configuration config = new MynockConfiguration( mynockInterval, mynockSelector, mynockMeanOutage );
            Mynock mynock = new Mynock( config, orchestrator, endCondition, remoteControl, reportingFactory, logProvider );
            mynocks.add( mynock );
            mynock.start();
        }
    }

    private void startSchemaWorker( Condition endCondition )
    {
        if ( schemaOperations.isEmpty() )
        {
            return;
        }

        SchemaWorker.Configuration config = new SchemaConfiguration( new Duration( schemaOperationsInterval, MILLISECONDS ), schemaOperations );
        schemaWorker = new SchemaWorker( config, orchestrator, endCondition, remoteControl, reportingFactory, logProvider );
        schemaWorker.start();
    }

    private void startRotator( Condition endCondition )
    {
        if ( rotationInterval == null )
        {
            return;
        }

        rotator = new RotatorWorker( new BaseWorkerConfiguration( rotationInterval, InstanceSelectors.leader ), orchestrator, endCondition, remoteControl,
                reportingFactory, logProvider );
        rotator.start();
    }

    private void startLeaderChecker( Condition endCondition )
    {
        Duration interval = new Duration( 5, SECONDS );
        LeaderChecker.Configuration config = new LeaderCheckerConfiguration( interval, InstanceSelectors.leader );
        leaderChecker = new LeaderChecker( config, orchestrator, endCondition, remoteControl, reportingFactory, logProvider );
        leaderChecker.start();
    }

    private void startKiller( Condition endCondition )
    {
        Killer.Configuration config = new KillerConfiguration( new Duration( killInterval, MILLISECONDS ), killSelector );
        killer = new Killer( config, orchestrator, endCondition, remoteControl, reportingFactory, instanceHealth, logProvider );
        killer.start();
    }

    private void startWorkers( Condition endCondition )
    {
        Worker.Configuration config = new BaseWorkerConfiguration( new Duration( opInterval, MILLISECONDS ), writeSelector );

        log.info( "Creating and starting workers" );
        for ( int i = 0; i < numberOfWorkers; i++ )
        {
            Worker worker = new Worker( "" + WORKER_ID.incrementAndGet(), config, orchestrator, endCondition, remoteControl, reportingFactory, logProvider );
            workers.add( worker );
            worker.start();
        }
    }

    private void catchAndRetryNTimes( long retryCount, int retryDelay, TimeUnit timeUnit, String name, ThrowingAction<Exception> operation ) throws Exception
    {
        while ( retryCount-- > 0 )
        {
            try
            {
                operation.apply();
                return;
            }
            catch ( Exception e )
            {
                if ( retryCount > 0 )
                {
                    log.warn( format( "Operation '%s' failed. Retrying.", name ), e );
                    Thread.sleep( timeUnit.toMillis( retryDelay ) );
                }
                else
                {
                    log.error( format( "Operation '%s' failed. Aborting.", name ), e );
                    throw e;
                }
            }
        }
    }

    private void createInitialDataset( Orchestrator orchestrator ) throws Exception
    {
        /*
         * There are three things that need to happen as part of the initial data set. First comes the bulk of data,
         * nodes, rels an props in big numbers. The second is index creation and finally comes constraint creation. Every
         * one of them has to happen against a leader - that's the constraint of CE right now. So we keep retrying each one
         * until it completes against the same leader. Failures can come from timeouts that cause elections and leader
         * switches, caused by slow machines, GC, whatever. The fact that some of them may happen against different machines
         * is not really a problem, since all cluster instances know of all committed txs, by design.
         */
        log.info( "Building initial data set (" + initialSize + ")..." );

        catchAndRetryNTimes( 5, 500, MILLISECONDS, "Initial data set", () ->
        {
            CcInstance leader = orchestrator.getLeaderInstance();
            leader.doBatchOfOperations( initialSize, Operation.createNode );
        } );

        catchAndRetryNTimes( 5, 500, MILLISECONDS, "Index creation", () ->
        {
            CcInstance leader = orchestrator.getLeaderInstance();
            createInitialSchema( leader, SchemaOperations.createIndex );
        } );

        catchAndRetryNTimes( 5, 500, MILLISECONDS, "Constraint creation", () ->
        {
            CcInstance leader = orchestrator.getLeaderInstance();
            createInitialSchema( leader, SchemaOperations.createUniquenessConstraint );
        } );
    }

    private void createInitialSchema( CcInstance leader, SchemaOperation schemaOperation ) throws RemoteException
    {
        if ( schemaOperations.contains( schemaOperation.type() ) )
        {
            int ops = 10;
            log.info( "Building initial schema of type " + schemaOperation.type() + " (" + ops + ")..." );
            for ( int i = 0; i < ops; i++ )
            {
                leader.doSchemaOperation( schemaOperation );
            }
        }
    }

    private Throwable waitForWorkersToExit() throws InterruptedException
    {
        Collection<Worker> allWorkers = addAll(
                new ArrayList<>(), Iterables.filter( notNull(), Iterables.iterable( killer, leaderChecker, rotator, schemaWorker ) ) );
        allWorkers.addAll( workers );
        allWorkers.addAll( mynocks );

        long maxWaitTime = MINUTES.toMillis( 10 );
        long maxEndTime = currentTimeMillis() + maxWaitTime;
        Throwable fatalError = this.fatalError;
        for ( Worker worker : allWorkers )
        {
            long leftToWait = maxEndTime - currentTimeMillis();
            if ( leftToWait > 0 )
            {
                worker.join( abs( min( maxWaitTime, leftToWait ) ) );
            }
        }
        return fatalError;
    }

    private CommandReactor startReactingToUserInput()
    {
        CommandReactor reactor = new CommandReactor( "CC robustness", logProvider );
        reactor.add( "pause", action -> remoteControl.pause( null ) );
        reactor.add( "pause-killer", action -> remoteControl.pauseKiller( null ) );
        reactor.add( "exit", action -> globalHalt = true );
        reactor.add(
                "kill",
                action -> killer.queue( action.orphans().get( 1 ), action.getEnum( ShutdownType.class, "type", null ), action.orphans().contains( "now" ) ) );
        reactor.add( "dump", action -> dumpAllInfo( orchestrator, "Manually requested dump" ) );
        return reactor;
    }

    private void startHealthCheck( final Condition endCondition )
    {
        statsExecutor.scheduleAtFixedRate( new Runnable()
        {
            private final Reporting reporting = reportingFactory.create( "Health check" );
            private boolean dumped;
            private boolean unresponsive;
            private boolean endConditionMet;
            private long lastSuccessCount;

            @Override
            public void run()
            {
                if ( endConditionMet || (endConditionMet = endCondition.met()) )
                {   // Check and cache whether or not the end condition has been met
                    return;
                }

                try
                {
                    int successCount = counts.get( TransactionOutcome.success ).count();
                    String status = (successCount - lastSuccessCount) + " " + counts + " " + average() + " " + orchestrator.getConvenientStatus() + " " +
                            workersString() + " " + consecutiveErrorStats( orchestrator );
                    lastSuccessCount = successCount;

                    log.info( "TXs " + status );
                    if ( pause.get() )
                    {
                        counts.get( TransactionOutcome.success ).ping();
                        return;
                    }

                    int secondsUnresponsive = counts.get( TransactionOutcome.success ).secondsSinceLastIncremented();
                    if ( secondsUnresponsive > limit( 20 ) )
                    {   // Nothing happened for a while
                        // Stop the up/down worker so that we don't rely on it to solve these
                        // unresponsiveness issues. We'd like the cluster to sort it out for itself.
                        if ( !unresponsive )
                        {
                            dumpAllInfo( orchestrator, "Starting to get unresponsive" );
                        }
                        unresponsive = true;
                        log.warn( "Cluster unresponsive " + secondsUnresponsive + "s" );
                        remoteControl.pauseKiller( true );
                        if ( secondsUnresponsive >= limit( maxClusterUnresponsiveTimeAllowed ) )
                        {
                            String message = "Cluster unresponsive to writes for quite some time now, " + secondsUnresponsive + "s";
                            if ( !dumped )
                            {
                                dumped = true;
                                reporting.fatalError( message, new Exception( message ) );
                            }
                        }
                    }
                    else
                    {
                        // Things are looking good and write transactions are coming through
                        if ( unresponsive )
                        {
                            log.info( "Cluster starting to respond again" );
                        }
                        unresponsive = false;
                        dumped = false;
                        remoteControl.pauseKiller( false );
                    }
                }
                catch ( Throwable e )
                {
                    log.info( "Error in status thread: " + e );
                }
            }

            private String consecutiveErrorStats( Orchestrator orchestrator )
            {
                int count = orchestrator.getNumberOfCcInstances();
                StringBuilder result = new StringBuilder( " CE {" );
                int instancesWithErrors = 0;
                for ( int i = 0; i < count; i++ )
                {
                    int errors = consecutiveExceptions.get( i ).get();
                    if ( errors > 0 )
                    {
                        result.append( instancesWithErrors++ > 0 ? "," : "" ).append( i ).append( ":" ).append( errors );
                    }
                }
                return instancesWithErrors > 0 ? result.append( "}" ).toString() : "";
            }

            private String average()
            {
                OperationCount successCount = counts.get( TransactionOutcome.success );
                int seconds = (int) ((currentTimeMillis() - startTime) / 1000);
                seconds = seconds == 0 ? 1 : seconds;
                int opsPerSecond = successCount.count() / seconds;
                return "avg " + opsPerSecond;
            }

            private String workersString()
            {
                StringBuilder builder = new StringBuilder( "{" );
                int count = 0;
                for ( Worker worker : workers )
                {
                    builder.append( builder.length() > 1 ? "," : "" ).append( worker.state() );
                    if ( ++count >= 10 )
                    {
                        break;
                    }
                }
                if ( workers.size() > count )
                {
                    builder.append( "..." ).append( workers.get( workers.size() - 1 ).state() );
                }
                return builder.append( "}" ).toString();
            }
        }, 2, 2, TimeUnit.SECONDS );
    }

    private long limit( long time )
    {
        return gentleness != Gentleness.not ? time * 3 : time;
    }

    private void dumpAllInfo( final Orchestrator orchestrator, String message )
    {
        try
        {
            log.error( message );
            orchestrator.dumpProcessInformation( false );
            CcInstance leaderCcInstance = orchestrator.getLeaderInstance();
            if ( leaderCcInstance != null )
            {
                leaderCcInstance.dumpLocks();
            }
            else
            {
                log.warn( "No master to dump locks from" );
            }

            orchestrator.dumpInfo( log );
        }
        catch ( Exception e )
        {   // Hmm
            log.error( "Unknown error while dumping information", e );
        }
    }

    @Override
    public void forceShutdown()
    {
        if ( globalHalt )
        {
            return;
        }
        globalHalt = true;
        try
        {
            waitForWorkersToExit();
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static class TooManyErrors extends Exception
    {
        TooManyErrors( int count, Throwable exception )
        {
            super( "Error encountered " + count + " times", exception );
        }
    }

    private class BaseWorkerConfiguration implements Worker.Configuration
    {
        private final Duration waitBetweenOperations;
        private final InstanceSelector instanceSelector;

        BaseWorkerConfiguration( Duration waitBetweenOperations, InstanceSelector instanceSelector )
        {
            this.waitBetweenOperations = waitBetweenOperations;
            this.instanceSelector = instanceSelector;
        }

        @Override
        public Duration waitBetweenOperations()
        {
            return waitBetweenOperations;
        }

        @Override
        public InstanceSelector instanceSelector()
        {
            return instanceSelector;
        }

        @Override
        public Gentleness gentleness()
        {
            return gentleness;
        }

        @Override
        public float bigTransactionProbability()
        {
            return bigTransactionProbability;
        }

        @Override
        public int transactionSize()
        {
            return txSize;
        }

        @Override
        public Duration waitAfterFailure()
        {
            return waitAfterFailure;
        }
    }

    private class LeaderCheckerConfiguration extends BaseWorkerConfiguration implements LeaderChecker.Configuration
    {
        LeaderCheckerConfiguration( Duration waitBetweenOperations, InstanceSelector instanceSelector )
        {
            super( waitBetweenOperations, instanceSelector );
        }

        @Override
        public int consecutiveFailuresAllowed()
        {
            return (int) limit( 15 );
        }
    }

    private class KillerConfiguration extends BaseWorkerConfiguration implements Killer.Configuration
    {
        KillerConfiguration( Duration waitBetweenOperations, InstanceSelector instanceSelector )
        {
            super( waitBetweenOperations, instanceSelector );
        }

        @Override
        public ShutdownTypeSelector shutdownTypeSelector()
        {
            return shutdownTypeSelector;
        }

        @Override
        public Duration waitBeforeResurrect()
        {
            return resurrectWait;
        }
    }

    private class SchemaConfiguration extends BaseWorkerConfiguration implements SchemaWorker.Configuration
    {
        private final Collection<SchemaOperationType> types;

        SchemaConfiguration( Duration duration, Collection<SchemaOperationType> types )
        {
            super( duration, InstanceSelectors.leader );
            this.types = types;
        }

        @Override
        public Collection<SchemaOperationType> schemaOperationTypes()
        {
            return types;
        }
    }

    private class FullRemoteControl implements RemoteControl
    {
        private final Listeners<RemoteControlListener> listeners = new Listeners<>();

        @Override
        public void stop()
        {
            listeners.notify( RemoteControlListener::stopped );
        }

        @Override
        public void pause( final Boolean pause )
        {
            final boolean value = pause != null ? pause : !FullOnWorkLoad.this.pause.get();
            boolean previous = FullOnWorkLoad.this.pause.getAndSet( value );
            if ( previous != value )
            {
                log.info( "Pause " + (pause != null ? "set" : "toggled") + " to " + value );
                listeners.notify( listener -> listener.pausedChanged( value ) );
            }
        }

        @Override
        public void pauseKiller( final Boolean pause )
        {
            final boolean value = pause != null ? pause : !FullOnWorkLoad.this.pauseKiller.get();
            boolean previous = FullOnWorkLoad.this.pauseKiller.getAndSet( value );
            if ( previous != value )
            {
                log.info( "Killer pause " + (pause != null ? "set" : "toggled") + " to " + value );
                listeners.notify( listener -> listener.killerPausedChanged( value ) );
            }
        }

        @Override
        public void addListener( RemoteControlListener listener )
        {
            listeners.add( listener );
        }

        @Override
        public void removeListener( RemoteControlListener listener )
        {
            listeners.remove( listener );
        }
    }

    private class FullReportingFactory implements ReportingFactory
    {
        @Override
        public Reporting create( final String workerName )
        {
            return new Reporting()
            {
                @Override
                public void startedTransaction()
                {
                }

                @Override
                public void succeededTransaction( int serverId )
                {
                    counts.get( TransactionOutcome.success ).increment();
                    clearErrors( serverId );
                }

                @Override
                public void failedTransaction( int serverId, Throwable t )
                {
                    TransactionOutcome outcome = orchestrator.categorizeFailure( serverId, t );

                    counts.get( outcome ).increment();
                    consecutiveExceptionsList.get( serverId ).add( t.getClass().getSimpleName() );

                    switch ( outcome )
                    {
                    case benign_failure:
                        log.info( workerName + " at instance " + serverId + " got exception " + t.toString() + "', waiting a while..." +
                                gatherAllExceptionMessages( t ) );
                        break;
                    case unknown_failure:
                        log.warn( workerName + " at instance " + serverId + " got exception " + t.toString() + "', waiting a while...", t );
                        break;
                    default:
                        throw new IllegalStateException();
                    }

                    AtomicInteger exceptionCount = consecutiveExceptions.get( serverId );
                    if ( exceptionCount.incrementAndGet() > workers.size() * limit( 120 ) )
                    {
                        Map<String,Integer> exceptionCounts = new HashMap<>();
                        for ( String exceptionName : consecutiveExceptionsList.get( serverId ) )
                        {
                            Integer count = exceptionCounts.get( exceptionName );
                            if ( count == null )
                            {
                                count = 0;
                            }
                            count += 1;
                            exceptionCounts.put( exceptionName, count );
                        }
                        Exception e = new TooManyErrors( exceptionCount.get(), t );
                        fatalError( serverId + " has had " + exceptionCount.get() + " exceptions in a row. Considering that a fatal error. Exception counts:" +
                                exceptionCounts, e );
                    }
                }

                @Override
                public void fatalError( String message, Throwable t )
                {
                    log.error( "Fatal error " + message + ":" + t, t );
                    dumpAllInfo( orchestrator, message );
                    globalHalt = true;
                    fatalError = t;
                }

                @Override
                public void done()
                {
                }

                @Override
                public void clearErrors( int serverId )
                {
                    consecutiveExceptions.get( serverId ).set( 0 );
                    consecutiveExceptionsList.get( serverId ).clear();
                }

                private String gatherAllExceptionMessages( Throwable e )
                {
                    StringBuilder builder = new StringBuilder();
                    while ( e != null )
                    {
                        if ( e.getMessage() != null )
                        {
                            builder.append( format( "%n  %s", e.toString() ) );
                        }
                        e = e.getCause();
                    }
                    return builder.toString();
                }
            };
        }
    }

    private class FullInstanceHealth implements InstanceHealth
    {
        @Override
        public boolean isOk( CcInstance instance )
        {
            try
            {
                return consecutiveExceptions.get( instance.getServerId() ).get() == 0;
            }
            catch ( RemoteException e )
            {
                return false;
            }
        }
    }

    public class MynockConfiguration extends BaseWorkerConfiguration implements Mynock.Configuration
    {
        private final Duration maxOutageTime;

        MynockConfiguration( Duration waitBetweenOperations, InstanceSelector instanceSelector, Duration maxOutageTime )
        {
            super( waitBetweenOperations, instanceSelector );
            this.maxOutageTime = maxOutageTime;
        }

        @Override
        public Duration cableOutageMaxTime()
        {
            return maxOutageTime;
        }
    }
}
