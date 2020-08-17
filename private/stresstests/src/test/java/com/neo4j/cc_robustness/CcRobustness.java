/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness;

import com.neo4j.cc_robustness.Orchestrator.JvmMode;
import com.neo4j.cc_robustness.workload.InstanceSelector;
import com.neo4j.cc_robustness.workload.InstanceSelectors;
import com.neo4j.cc_robustness.workload.ReferenceNodeStrategy;
import com.neo4j.cc_robustness.workload.ScriptedInstanceSelector;
import com.neo4j.cc_robustness.workload.ShutdownTypeSelector;
import com.neo4j.cc_robustness.workload.WorkLoad;
import com.neo4j.cc_robustness.workload.full.FullOnWorkLoad;
import com.neo4j.cc_robustness.workload.step.RandomStepsGenerator;
import com.neo4j.cc_robustness.workload.step.StepByStepWorkLoad;
import org.apache.lucene.store.AlreadyClosedException;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Predicate;

import org.neo4j.function.Predicates;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.Args;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LogConfig;

import static java.lang.Math.abs;
import static java.lang.Math.min;
import static java.lang.System.exit;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.lock_manager;
import static org.neo4j.configuration.GraphDatabaseSettings.dense_node_threshold;

public class CcRobustness
{
    public static final int DEFAULT_CLUSTER_SIZE = 3;
    public static final int DEFAULT_TIME_IN_BETWEEN = 4000;
    public static final int DEFAULT_TX_SIZE = 10;
    public static final int DEFAULT_NUM_THREADS = 12;
    public static final JvmMode DEFAULT_JVM_MODE = JvmMode.same;
    public static final InstanceSelector DEFAULT_WRITE_MODE = InstanceSelectors.leader;
    public static final InstanceSelector DEFAULT_KILL_SELECTOR = InstanceSelectors.any;
    public static final ShutdownTypeSelector DEFAULT_SHUTDOWN_TYPE = ShutdownTypeSelector.random;
    public static final WorkLoadInstantiator DEFAULT_WORKLOAD = WorkLoadInstantiator.full;
    public static final ReferenceNodeStrategy DEFAULT_REFERENCE_NODE = ReferenceNodeStrategy.mixed;
    public static final boolean DEFAULT_INDEXING = true;
    public static final boolean DEFAULT_KEEP_DB_HISTORY = false;
    public static final boolean DEFAULT_ACQUIRE_READ_LOCKS = false;
    public static final int DEFAULT_MYNOCKS = 0;
    public static final InstanceSelector DEFAULT_MYNOCK_SELECTOR = InstanceSelectors.any;
    public static final String DEFAULT_MYNOCK_INTERVAL = "50s";
    public static final String DEFAULT_SLOW_LOGGING_LOGGERS = "org.neo4j.cluster";
    public static final String DEFAULT_SLOW_LOGGING_PROBABILITY = "0.0";

    public static final String FOREVER = "forever";
    public static final String STEPS = "steps";
    public static final String WORKLOAD = "workload";
    public static final String CLUSTER_SIZE = "cluster-size";
    public static final String NUMBER_OF_RUNS = "runs";
    public static final String TIME_IN_BETWEEN = "time-in-between";
    public static final String TX_SIZE = "tx-size";
    public static final String THREADS = "threads";
    public static final String TIME = "time";
    public static final String JVM_MODE = "jvm-mode";
    public static final String WRITE_MODE = "write-mode";
    public static final String KILL_SELECTOR = "kill";
    public static final String SHUTDOWN_TYPE = "shutdown-type";
    public static final String REFERENCENODE = "referencenode";
    public static final String INDEXING = "indexing";
    public static final String KEEP_DB_HISTORY = "history";
    public static final String ROTATE = "rotations";
    public static final String ACQUIRE_READ_LOCKS = "acquire-read-locks";
    public static final String MYNOCKS = "mynocks";
    public static final String MYNOCK_MEAN_OUTAGE_TIME = "mynock-mean-outage-time";
    public static final String MYNOCK_SELECTOR = "mynock-selector";
    public static final String MYNOCK_INTERVAL = "mynock-interval";
    public static final String SLOW_LOGGING_PROBABILITY = "slow-logging-probability";
    public static final String SLOW_LOGGING_LOGGERS = "slow-logging-loggers";
    private static final Function<String,Integer> parseIntOrRandomInRange = from ->
    {
        try
        {
            return Integer.parseInt( from );
        }
        catch ( NumberFormatException e )
        {
            int dashIndex = from.indexOf( '-' );
            if ( dashIndex != -1 )
            {   // E.g. "10-50"
                int low = Integer.parseInt( from.substring( 0, dashIndex ) );
                int high = Integer.parseInt( from.substring( dashIndex + 1 ) );
                return new Random().nextInt( abs( high - low ) ) + min( low, high );
            }
            throw e;
        }
    };

    public static void main( String[] argArray )
    {
        try ( var logProvider = new Log4jLogProvider( LogConfig.createBuilder( System.out, Level.INFO ).withTimezone( LogTimeZone.UTC ).build() ) )
        {
            Log log = logProvider.getLog( CcRobustness.class );

            Args args = Args.parse( argArray );
            Predicate<Void> keepOnGoing = instantiateGoer( args.get( NUMBER_OF_RUNS, "1" ) );
            for ( int i = 0; keepOnGoing.test( null ); i++ )
            {
                log.info( "=== RUN " + i + " ===" );
                try
                {
                    oneRun( args, logProvider );
                }
                catch ( AlreadyClosedException e )
                {
                    // dully ignored
                }
                catch ( Exception e )
                {
                    log.error( "Got exception: ", e );
                    log.error( "I don't know how to deal with it. Exiting with status code 1." );
                    System.out.flush();
                    System.err.flush();
                    exit( 1 );
                }
                finally
                {
                    System.out.flush();
                    System.err.flush();
                }
            }
        }
    }

    private static Predicate<Void> instantiateGoer( final String string )
    {
        if ( string.equals( FOREVER ) )
        {
            return Predicates.alwaysTrue();
        }

        return new Predicate<>()
        {
            private int number = Integer.parseInt( string );

            @Override
            public boolean test( Void item )
            {
                return number-- > 0;
            }
        };
    }

    private static void oneRun( Args args, LogProvider logProvider ) throws Exception
    {
        Log log = logProvider.getLog( CcRobustness.class );
        int clusterSize = args.getNumber( CLUSTER_SIZE, DEFAULT_CLUSTER_SIZE ).intValue();
        ReferenceNodeStrategy referenceNodeStrategy = ReferenceNodeStrategy.valueOf( args.get( REFERENCENODE, DEFAULT_REFERENCE_NODE.name() ) );
        boolean indexing = args.getBoolean( INDEXING, DEFAULT_INDEXING, true );
        boolean keepDbHistory = args.getBoolean( KEEP_DB_HISTORY, DEFAULT_KEEP_DB_HISTORY, true );
        boolean acquireReadLocks = args.getBoolean( ACQUIRE_READ_LOCKS, DEFAULT_ACQUIRE_READ_LOCKS, true );
        Map<String,String> additionalDbConfig = parseAdditionalDbConfig( args );
        Orchestrator orchestrator = new Orchestrator( logProvider, clusterSize, JvmMode.valueOf( args.get( JVM_MODE, DEFAULT_JVM_MODE.name() ) ), null,
                keepDbHistory, referenceNodeStrategy, indexing, additionalDbConfig, acquireReadLocks );
        log.info( "Provided arguments: " + args.asMap().toString() );
        log.info( "Parsed additional db config: " + additionalDbConfig );
        try
        {
            WorkLoadInstantiator instantiator = WorkLoadInstantiator.valueOf( args.get( WORKLOAD, DEFAULT_WORKLOAD.name() ) );
            WorkLoad workLoad = instantiator.instantiate( args, logProvider );

            // Run the work load
            log.info( "Performing workload..." );
            orchestrator.perform( workLoad );
            log.info( "Workload complete." );
        }
        catch ( Exception e )
        {
            log.info( "Ended in exception", e );
            throw e;
        }
        finally
        {
            boolean allEqual;
            try
            {
                allEqual = orchestrator.shutdown();
            }
            catch ( Exception e )
            {
                // TODO Rethrowing the exception here, but should we instead catch it and do
                // what's left of this method in any case?
                orchestrator.dumpProcessInformation( false );
                throw e;
            }

            orchestrator.verify();
            if ( !allEqual )
            {
                throw new IllegalStateException( "Contents differ between instances" );
            }
        }

        // We made a successful run so we don't have to keep the artifacts around
        log.info( "Everything verified and OK" );
        orchestrator.cleanArtifacts();
    }

    private static Map<String,String> parseAdditionalDbConfig( Args args )
    {
        Map<String,String> params = new HashMap<>();

        populateFromArgs( params, args, lock_manager );

        int denseNodeThreshold = parseIntOrRandomInRange.apply( args.get( dense_node_threshold.name(), "10-50" ) );
        params.put( dense_node_threshold.name(), "" + denseNodeThreshold );

        return params;
    }

    private static void populateFromArgs( Map<String,String> params, Args args, Setting<?> setting )
    {
        String value = args.get( setting.name(), null );
        if ( value != null )
        {
            params.put( setting.name(), value );
        }
    }

    protected static InstanceSelector getInstanceSelector( String string )
    {
        try
        {
            return InstanceSelectors.selector( string );
        }
        catch ( IllegalArgumentException e )
        {
            return new ScriptedInstanceSelector( string );
        }
    }

    private enum WorkLoadInstantiator
    {
        full
                {
                    @Override
                    WorkLoad instantiate( Args args, LogProvider logProvider )
                    {
                        return new FullOnWorkLoad( args.getNumber( TIME, 2 * week() ).intValue(), args.getNumber( THREADS, DEFAULT_NUM_THREADS ).intValue(),
                                args.getNumber( TX_SIZE, DEFAULT_TX_SIZE ).intValue(), args.getNumber( CLUSTER_SIZE, DEFAULT_CLUSTER_SIZE ).intValue(),
                                getInstanceSelector( args.get( KILL_SELECTOR, DEFAULT_KILL_SELECTOR.name() ) ),
                                ShutdownTypeSelector.valueOf( args.get( SHUTDOWN_TYPE, DEFAULT_SHUTDOWN_TYPE.name() ) ), args,
                                InstanceSelectors.valueOf( args.get( WRITE_MODE, DEFAULT_WRITE_MODE.name() ) ) );
                    }

                    private int week()
                    {
                        return 60/*minute*/ * 60/*hour*/ * 24/*day*/ * 7/*week*/;
                    }
                },
        random_steps
                {
                    @Override
                    WorkLoad instantiate( Args args, LogProvider logProvider )
                    {
                        int clusterSize = args.getNumber( CLUSTER_SIZE, DEFAULT_CLUSTER_SIZE ).intValue();
                        long timeInBetween = args.getNumber( TIME_IN_BETWEEN, DEFAULT_TIME_IN_BETWEEN ).longValue();
                        long seed = new Random().nextLong();
                        logProvider.getLog( WorkLoadInstantiator.class ).info( "Running with work load 'random_steps' using seed: " + seed );
                        return new StepByStepWorkLoad( clusterSize, timeInBetween, new StepByStepWorkLoad.VerifyConsistency(),
                                new RandomStepsGenerator( clusterSize, seed ).generateRandomSteps( 15, 30 ) );
                    }
                },
        steps
                {
                    @Override
                    WorkLoad instantiate( Args args, LogProvider logProvider )
                    {
                        int clusterSize = args.getNumber( CLUSTER_SIZE, DEFAULT_CLUSTER_SIZE ).intValue();
                        long timeInBetween = args.getNumber( TIME_IN_BETWEEN, DEFAULT_TIME_IN_BETWEEN ).longValue();
                        return new StepByStepWorkLoad( clusterSize, timeInBetween, new StepByStepWorkLoad.VerifyConsistency(),
                                StepByStepWorkLoad.parse( args.get( STEPS, null ) ) );
                    }
                };

        abstract WorkLoad instantiate( Args args, LogProvider logProvider );
    }
}
