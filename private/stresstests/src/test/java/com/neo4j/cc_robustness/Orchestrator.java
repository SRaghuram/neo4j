/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness;

import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.roles.RoleProvider;
import com.neo4j.causalclustering.core.consensus.state.ExposedRaftState;
import com.neo4j.causalclustering.core.state.CommandApplicationProcess;
import com.neo4j.causalclustering.core.state.CoreInstanceInfo;
import com.neo4j.cc_robustness.consistency.RobustnessConsistencyCheck;
import com.neo4j.cc_robustness.workload.ReferenceNodeStrategy;
import com.neo4j.cc_robustness.workload.ShutdownType;
import com.neo4j.cc_robustness.workload.WorkLoad;
import com.neo4j.cc_robustness.workload.full.TransactionFailureConditions;
import com.neo4j.cc_robustness.workload.full.TransactionOutcome;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.configuration.OnlineBackupSettings;
import org.apache.lucene.util.NamedThreadFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.rmi.RemoteException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Supplier;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.batchimport.cache.idmapping.string.Workers;
import org.neo4j.kernel.impl.locking.DumpLocksVisitor;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.lock.AcquireLockTimeoutException;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;

import static com.neo4j.cc_robustness.CcInstance.NF_IN;
import static com.neo4j.cc_robustness.CcInstance.NF_OUT;
import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.internal.helpers.Format.time;
import static org.neo4j.internal.helpers.collection.MapUtil.load;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;

public class Orchestrator
{
    public static final File DEFAULT_PATH = new File( "data" );
    public static final File CONFIG_FILE = new File( "neo4j.properties" );
    private static final int LOW_BACKUP_SERVER_PORT = 4000;
    private static final int LOW_DISCOVERY_SERVER_PORT = 5000;
    private static final int LOW_TX_SERVER_PORT = 6000;
    private static final int LOW_RAFT_SERVER_PORT = 7000;
    private static final int LOW_BOLT_SERVER_PORT = 8000;
    private final CcInstanceFiles instanceFiles;
    private final RobustnessConsistencyCheck consistencyVerifier;
    private final JvmMode jvmMode;
    private final int numberOfCcInstances;
    private final Random random = new Random();
    private final File path;
    private final ReferenceNodeStrategy referenceNodeStrategy;
    private final boolean indexing;
    private final LogProvider logProvider;
    private final Log log;
    private final EqualContentsVerifier equalContents;
    private final Config additionalDbConfig;
    private final DumpProcessInformation processInformationDumper;
    private final boolean acquireReadLocks;
    private final TransactionFailureConditions failureConditions = new TransactionFailureConditions();
    private final Map<String,String> rawAdditionalConfigMap;
    private AtomicReferenceArray<Instance> ccInstances;
    private WorkLoad currentLoad;
    private boolean workLoadIsCompleted;

    public Orchestrator( LogProvider logProvider, int numberOfCcInstances, JvmMode jvmMode, File pathOrNullForDefault, boolean keepDbHistory,
            ReferenceNodeStrategy referenceNodeStrategy, boolean indexing, Map<String,String> additionalDbConfig, boolean acquireReadLocks ) throws Exception
    {
        this.logProvider = logProvider;
        rawAdditionalConfigMap = getRawAdditionalConfigMap( numberOfCcInstances, additionalDbConfig );
        this.additionalDbConfig = Config.newBuilder().setRaw( rawAdditionalConfigMap ).build();
        this.acquireReadLocks = acquireReadLocks;
        this.log = logProvider.getLog( getClass() );
        this.numberOfCcInstances = numberOfCcInstances;
        this.jvmMode = jvmMode;
        this.referenceNodeStrategy = referenceNodeStrategy;
        this.indexing = indexing;
        this.path = pathOrNullForDefault != null ? pathOrNullForDefault : DEFAULT_PATH;
        this.consistencyVerifier = new RobustnessConsistencyCheck( logProvider.getLog( RobustnessConsistencyCheck.class ) );
        this.equalContents = new EqualContentsVerifier( log );
        this.instanceFiles = new CcInstanceFiles( path, log, keepDbHistory );
        this.processInformationDumper = new DumpProcessInformation( logProvider, path );

        instanceFiles.init();

        log.info( "Starting CC cluster..." );
        if ( !CONFIG_FILE.exists() )
        {
            log.debug( "(You can specify custom configuration for instances in neo4j.properties file here in the root directory)" );
        }
        else
        {
            log.debug( "(Using custom configuration from neo4j.properties " + loadConfig() + ")" );
        }
        initializeCcInstances( numberOfCcInstances );
    }

    private static Map<String,String> loadUserConfig() throws IOException
    {
        Map<String,String> config = stringMap();
        if ( CONFIG_FILE.exists() )
        {
            config.putAll( loadConfig() );
        }
        return config;
    }

    private static Map<String,String> loadConfig() throws IOException
    {
        try ( FileInputStream stream = new FileInputStream( CONFIG_FILE ) )
        {
            return load( stream );
        }
    }

    private static Map<String,String> combineConfig( Map<String,String> first, Map<String,String> other )
    {
        Map<String,String> result = new HashMap<>( first );
        result.putAll( other );
        return result;
    }

    static CoreInstanceInfo coreInfo( GraphDatabaseAPI db )
    {
        DependencyResolver dependencyResolver = db.getDependencyResolver();
        ExposedRaftState state = dependencyResolver.resolveDependency( RaftMachine.class ).state();
        CommandApplicationProcess applicationProcess = dependencyResolver.resolveDependency( CommandApplicationProcess.class );

        return new CoreInstanceInfo( state, applicationProcess );
    }

    public static boolean isLeader( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( RoleProvider.class ).currentRole() == Role.LEADER;
    }

    static DatabaseManagementService instantiateDbServer( Path homeDir, int serverId, Map<String,String> additionalConfig,
            CcStartupTimeoutMonitor ccStartupTimeoutMonitor )
    {
        Throwable startupFailure = null;
        int retries = 4;
        while ( retries-- > 0 )
        {
            try
            {
                Monitors monitors = new Monitors();
                monitors.addMonitorListener( ccStartupTimeoutMonitor );
                return new CcRobustnessGraphDatabaseFactory( homeDir )
                        .setConfigRaw( additionalConfig )
                        .setMonitors( monitors )
                        .setConfig( GraphDatabaseSettings.neo4j_home, homeDir.toAbsolutePath() )
                        .setConfig( GraphDatabaseInternalSettings.databases_root_path, homeDir.resolve( "data/databases" ) )
                        .setConfig( GraphDatabaseSettings.check_point_interval_time, Duration.ofMinutes( 2 ) )
                        .setConfig( CausalClusteringSettings.raft_listen_address, new SocketAddress( "127.0.0.1", LOW_RAFT_SERVER_PORT + serverId ) )
                        .setConfig( CausalClusteringSettings.raft_advertised_address, new SocketAddress( LOW_RAFT_SERVER_PORT + serverId ) )
                        .setConfig( CausalClusteringSettings.transaction_listen_address, new SocketAddress( "127.0.0.1", LOW_TX_SERVER_PORT + serverId ) )
                        .setConfig( CausalClusteringSettings.transaction_advertised_address, new SocketAddress( LOW_TX_SERVER_PORT + serverId ) )
                        .setConfig( CausalClusteringSettings.discovery_listen_address, new SocketAddress( "127.0.0.1", LOW_DISCOVERY_SERVER_PORT + serverId ) )
                        .setConfig( CausalClusteringSettings.discovery_advertised_address, new SocketAddress( LOW_DISCOVERY_SERVER_PORT + serverId ) )
                        .setConfig( OnlineBackupSettings.online_backup_listen_address, new SocketAddress( "127.0.0.1", LOW_BACKUP_SERVER_PORT + serverId ) )
                        .setConfig( CausalClusteringSettings.join_catch_up_timeout, Duration.ofMinutes( 30 ) )
                        .setConfig( BoltConnector.enabled, true )
                        .setConfig( BoltConnector.advertised_address, new SocketAddress( "127.0.0.1", LOW_BOLT_SERVER_PORT + serverId ) )
                        .setConfig( BoltConnector.listen_address, new SocketAddress( "127.0.0.1", LOW_BOLT_SERVER_PORT + serverId ) )
                        .setConfig( CausalClusteringSettings.middleware_logging_level, Level.DEBUG )
                        .build();
            }
            catch ( RuntimeException e )
            {
                startupFailure = e;
                Throwable current = e;
                while ( true )
                {
                    if ( current.getCause() == null )
                    {
                        throw new RuntimeException( e );
                    }

                    current = current.getCause();
                }
            }
        }

        throw new IllegalStateException(
                "Tried starting instance " + serverId + " multiple times, but it was " + "continuously denied entry.", startupFailure );
    }

    static void awaitStarted( Supplier<GraphDatabaseService> dbPoller, CcStartupTimeoutMonitor startupMonitor, int serverId )
    {
        // Wait for db to be assigned
        boolean interrupted = false;

        startupMonitor.start();

        while ( dbPoller.get() == null || !dbPoller.get().isAvailable( 1 ) )
        {
            long endTime = startupMonitor.getApplicableEndTimeMillis();
            if ( System.currentTimeMillis() > endTime )
            {
                throw new RuntimeException( "Waiting for database " + serverId + " to start, timeout " + startupMonitor );
            }

            try
            {
                Thread.sleep( 100 );
            }
            catch ( InterruptedException e )
            {
                interrupted = true;
                Thread.interrupted();
            }
        }
        if ( interrupted )
        {
            Thread.currentThread().interrupt();
        }

        // Wait for db to be available
        int maxDbAvailableWaitSeconds = (int) MINUTES.toSeconds( 20 );
        if ( !dbPoller.get().isAvailable( SECONDS.toMillis( maxDbAvailableWaitSeconds ) ) )
        {
            throw new RuntimeException(
                    "Waiting for database " + serverId + " to become available, timeout " + "reached at " + maxDbAvailableWaitSeconds + " seconds." );
        }
    }

    private static String initialHosts( int count )
    {
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < count; i++ )
        {
            builder.append( i > 0 ? "," : "" ).append( "localhost:" ).append( LOW_DISCOVERY_SERVER_PORT + i );
        }
        return builder.toString();
    }

    private static String timeStampedLocksDumpFileName()
    {
        return time().replace( ':', '_' ).replace( '.', '_' ) + "-locks-dump.txt";
    }

    static void dumpAllLocks( GraphDatabaseAPI db, Path dir, Log log )
    {
        Path outputFile = dir.resolve( timeStampedLocksDumpFileName() ).toAbsolutePath();

        log.info( "Dumping locks to: " + outputFile );
        DependencyResolver resolver = db.getDependencyResolver();

        try ( BufferedWriter writer = Files.newBufferedWriter( outputFile, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND ) )
        {
            FormattedLogProvider logProvider = FormattedLogProvider.toWriter( writer );
            resolver.resolveDependency( Locks.class ).accept( new DumpLocksVisitor( logProvider.getLog( Locks.class ) ) );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }

    static boolean isAvailable( GraphDatabaseAPI db )
    {
        return db.isAvailable( 100 );
    }

    public static void log( GraphDatabaseAPI db, String message )
    {
        db.getDependencyResolver().resolveDependency( LogService.class ).getInternalLog( CcInstance.class ).info( message );
    }

    private Map<String,String> getRawAdditionalConfigMap( int numberOfCcInstances, Map<String,String> additionalDbConfig ) throws IOException
    {
        return combineConfig( loadUserConfig(),
                stringMap( additionalDbConfig, CausalClusteringSettings.initial_discovery_members.name(), initialHosts( numberOfCcInstances ),
                        CausalClusteringSettings.raft_log_implementation.name(), "SEGMENTED" ) );
    }

    public <T> T getDbConfig( Setting<T> setting )
    {
        return additionalDbConfig.get( setting );
    }

    public void dumpProcessInformation( boolean includingHeapDump )
    {
        try
        {
            processInformationDumper.dumpRunningProcesses( includingHeapDump, CcRobustness.class.getName(), SubProcess.class.getName() );
        }
        catch ( Exception e )
        {
            System.err.println( "Couldn't dump process information" );
            e.printStackTrace();
        }
    }

    private void initializeCcInstances( int numberOfCcInstances ) throws Exception
    {
        log.info( "Starting up initial instances (" + numberOfCcInstances + ")" );
        ccInstances = new AtomicReferenceArray<>( numberOfCcInstances );

        bootstrapInstances( numberOfCcInstances );

        boolean done = false;
        long txTimeout = MILLISECONDS.toNanos( 30_000 );
        long start = nanoTime();
        while ( !done && (nanoTime() - start) < txTimeout )
        {
            try
            {
                getLeaderInstance().createReferenceNode();
                done = true;
            }
            catch ( TransactionFailureException | AcquireLockTimeoutException e )
            {
                log.warn( "Replication failed on initial dataset, retrying ", e );
            }
            Thread.sleep( 1000 );
        }

        log.info( "Initial instances started." );
    }

    private void bootstrapInstances( int numberOfCcInstances ) throws Exception
    {
        ExecutorService executorService = Executors.newFixedThreadPool( numberOfCcInstances, new NamedThreadFactory( "CC robustness instance bootstrapper" ) );
        try
        {
            Set<Future<?>> futures = new HashSet<>();
            for ( int serverId = 0; serverId < numberOfCcInstances; serverId++ )
            {
                ccInstances.set( serverId, new Instance() );
                final int finalServerId = serverId;
                Future<?> future = executorService.submit( () ->
                {
                    try
                    {
                        startCcInstance( finalServerId );
                    }
                    catch ( Exception e )
                    {
                        throw new RuntimeException( e );
                    }
                } );

                futures.add( future );
            }

            for ( Future<?> future : futures )
            {
                // We wait this long, because a recovery might occur, which can take time.
                future.get( 30, TimeUnit.MINUTES );
            }

            // Above we've started the databases in the cluster simultaneously. Each instance (CcInstance)
            // is started with a CcStartupTimeoutMonitor that waits proper times in the different phases
            // of a startup. So arriving here means that they've all gone through startup and formed a cluster.
            for ( int i = 0; i < ccInstances.length(); i++ )
            {
                ccInstances.get( i ).ccInstance.awaitStarted();
            }
        }
        finally
        {
            executorService.shutdown();
        }
    }

    private void startBringingUpInstance( int serverId )
    {
        if ( !isInstanceShutdown( serverId ) )
        {
            throw new IllegalStateException( "Instance " + serverId + " already started" );
        }
        ccInstances.get( serverId ).bringingUp();
    }

    public void shutdownCcInstance( int serverId, ShutdownType type, boolean waitForTransactions ) throws Exception
    {
        if ( isInstanceShutdown( serverId ) )
        {
            throw new IllegalStateException( "Instance " + serverId + " not running" );
        }
        ccInstances.get( serverId ).shutDown( type, waitForTransactions );
    }

    public boolean isInstanceShutdown( int serverId )
    {
        return ccInstances.get( serverId ).isShutDown();
    }

    public CcInstance startCcInstance( int serverId ) throws Exception
    {
        startBringingUpInstance( serverId );

        CcInstance instance = jvmMode.start( instanceFiles, consistencyVerifier, serverId, referenceNodeStrategy, indexing, rawAdditionalConfigMap, logProvider,
                acquireReadLocks );
        bringUpInstance( serverId, instance );

        return instance;
    }

    private void bringUpInstance( int serverId, CcInstance instance )
    {
        ccInstances.get( serverId ).up( instance );
    }

    public int getNumberOfCcInstances()
    {
        return numberOfCcInstances;
    }

    public int getLeaderServerId()
    {
        try
        {
            for ( int i = 0; i < numberOfCcInstances; i++ )
            {
                Instance instance = ccInstances.get( i );
                if ( instance != null && instance.isLeader() )
                {
                    return i;
                }
            }
            return -1;
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    public CcInstance getLeaderInstance()
    {
        long leaderWaitTimeout = MILLISECONDS.toNanos( 30_000 );
        long start = nanoTime();
        while ( (nanoTime() - start) < leaderWaitTimeout )
        {
            int id = getLeaderServerId();
            if ( id != -1 )
            {
                return getCcInstance( id );
            }
        }
        throw new IllegalStateException( format( "Could not find leader within %d ms.", leaderWaitTimeout ) );
    }

    private int getRandomFollowerServerId()
    {
        try
        {
            while ( true )
            {
                CcInstance instance = getRandomCcInstance();
                if ( instance != null && !instance.isLeader() )
                {
                    return instance.getServerId();
                }
            }
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    public CcInstance getRandomFollowerCcInstance()
    {
        return getCcInstance( getRandomFollowerServerId() );
    }

    void perform( WorkLoad load ) throws Exception
    {
        this.currentLoad = load;
        load.perform( this, logProvider );
        this.workLoadIsCompleted = true;
    }

    private int getRandomCcServerId()
    {
        return random.nextInt( ccInstances.length() );
    }

    public CcInstance getRandomCcInstance()
    {
        return ccInstances.get( getRandomCcServerId() ).cc();
    }

    public Instance getInstance( int serverId )
    {
        return ccInstances.get( serverId );
    }

    public CcInstance getCcInstance( int serverId )
    {
        return getInstance( serverId ).ccInstance;
    }

    public Iterable<CcInstance> getAliveCcInstances()
    {
        List<CcInstance> result = new ArrayList<>();
        for ( int i = 0; i < ccInstances.length(); i++ )
        {
            CcInstance instance = ccInstances.get( i ).cc();
            if ( instance != null )
            {
                result.add( instance );
            }
        }
        return result;
    }

    public boolean shutdown() throws Exception
    {
        log.info( "Shutting down" );
        if ( !workLoadIsCompleted )
        {
            log.info( "Forcing WorkLoad to shut down" );
            currentLoad.forceShutdown();
            workLoadIsCompleted = true;
        }

        dumpInfo( log );

        log.info( "Shutting down instances..." );
        shutdownInstances( false );

        try
        {
            log.info( "Waiting for instances to shut down..." );
            ensureInstancesAreShutDown();
            log.info( "Instances are all shut down." );
        }
        catch ( Exception e )
        {
            log.error( "Couldn't shut down all instances" );
            e.printStackTrace();
            throw e;
        }

        try
        {
            log.info( "Starting instances for verification..." );
            startInstancesForVerification();
            log.info( "Instances started, ready for verification." );
        }
        catch ( Exception e )
        {
            log.error( "Couldn't start all instances for verification", e );
            throw e;
        }

        Exception contentsException = null;
        boolean allEqual = false;
        try
        {
            allEqual = equalContents.verifyEqualContents( getAliveCcInstances() );
            if ( allEqual )
            {
                log.info( "All instances have same contents" );
            }
        }
        catch ( Exception e )
        {
            log.error( "Instance have different contents", e );
            contentsException = e;
        }
        shutdownInstances( true );
        log.info( "Everything shut down" );
        if ( contentsException != null )
        {
            throw contentsException;
        }
        return allEqual;
    }

    void ensureInstancesAreShutDown() throws Exception
    {
        for ( int i = 0; i < numberOfCcInstances; i++ )
        {
            if ( getCcInstance( i ) == null )
            {
                waitForInstanceToShutDown( i );
            }
        }
    }

    private void startInstancesForVerification() throws Exception
    {
        log.info( "Starting up instances (" + numberOfCcInstances + ") for verification" );

        bootstrapInstances( numberOfCcInstances );

        log.info( "Instances ready for verification." );
    }

    private void waitForInstanceToShutDown( int i ) throws Exception
    {
        Instance instance = ccInstances.get( i );

        log.info( "Waiting for instance " + i + " to shut down..." );
        instance.awaitShutDown( 2, TimeUnit.MINUTES );
        log.info( "Instance " + i + " has been shut down." );
    }

    void verify() throws Exception
    {
        for ( int i = 0; i < getNumberOfCcInstances(); i++ )
        {
            File homeDir = instanceFiles.directoryFor( i );
            try
            {
                consistencyVerifier.verifyConsistencyOffline( homeDir.toPath() );
                log.info( "VERIFIED " + homeDir );
            }
            catch ( Exception e )
            {
                log.error( "BROKEN STORE " + homeDir );
                e.printStackTrace();
                throw e;
            }
        }
    }

    void shutdownInstances( boolean nullList ) throws InterruptedException
    {
        if ( ccInstances == null )
        {
            return;
        }
        Workers<Runnable> shutters = new Workers<>( "Shutter" );
        for ( int i = 0; i < ccInstances.length(); i++ )
        {
            final Instance instance = ccInstances.get( i );
            final int index = i;
            if ( instance != null )
            {
                shutters.start( () ->
                {
                    try
                    {
                        log.info( "Shutting down instance " + index + "..." );
                        instance.shutDown( ShutdownType.clean, false );
                        log.info( "Instance " + index + " shut down" );
                    }
                    catch ( RemoteException e )
                    {
                        throw new RuntimeException( e );
                    }
                } );
            }
        }
        shutters.awaitAndThrowOnError();
        if ( nullList )
        {
            ccInstances = null;
        }
    }

    public String getConvenientStatus()
    {
        try
        {
            StringBuilder builder = new StringBuilder( "{" );
            for ( int i = 0; i < getNumberOfCcInstances(); i++ )
            {
                Instance instance = getInstance( i );
                builder.append( i > 0 ? ", " : "" ).append( i ).append( instance.phase.shortName() );
                instance.augmentStatus( builder );
            }
            return builder.append( "}" ).append( " " ).append( getNumberOfTotalBranches() ).append( "br" ).toString();
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    private int getNumberOfTotalBranches()
    {
        int count = 0;
        for ( int i = 0; i < numberOfCcInstances; i++ )
        {
            count += EmbeddedCcInstance.getNumberOfBranches( instanceFiles.directoryFor( i ).getAbsoluteFile() );
        }
        return count;
    }

    void cleanArtifacts()
    {
        instanceFiles.clean();
    }

    /**
     * Logs a message to all instances, into their messages.log
     */
    public void log( String message )
    {
        for ( CcInstance instance : getAliveCcInstances() )
        {
            try
            {
                instance.log( message );
            }
            catch ( RemoteException e )
            {
                // "Fine"
            }
        }
    }

    public TransactionOutcome categorizeFailure( int serverId, Throwable e )
    {
        if ( ccInstances.get( serverId ).isShutDown() )
        {
            return TransactionOutcome.boring_failure;
        }
        return failureConditions.apply( e );
    }

    public void dumpInfo( Log log )
    {
        log.info( "Failure distribution:\n" + failureConditions );
    }

    public enum JvmMode
    {
        same
                {
                    @Override
                    CcInstance start( CcInstanceFiles files, RobustnessConsistencyCheck consistencyCheck, int serverId,
                            ReferenceNodeStrategy referenceNodeStrategy, boolean indexing, Map<String,String> additionalDbConfig, LogProvider logProvider,
                            boolean acquireReadLocks ) throws Exception
                    {
                        CcInstance instance = new EmbeddedCcInstance( files, serverId, referenceNodeStrategy, consistencyCheck, additionalDbConfig, logProvider,
                                acquireReadLocks );
                        instance.awaitStarted();
                        return instance;
                    }
                },
        separate
                {
                    @Override
                    CcInstance start( CcInstanceFiles files, RobustnessConsistencyCheck consistencyCheck, int serverId,
                            ReferenceNodeStrategy referenceNodeStrategy, boolean indexing, Map<String,String> additionalDbConfig, LogProvider logProvider,
                            boolean grabReadLocks ) throws Exception
                    {
                        Map<String,String> parameters = stringMap( CcProcess.SERVER_ID_KEY, "" + serverId, "dbdir",
                                files.directoryFor( serverId ).getAbsolutePath(), GraphDatabaseSettings.logs_directory.name(),
                                files.directoryFor( serverId ).getAbsolutePath(), CcRobustness.REFERENCENODE, referenceNodeStrategy.name(),
                                CcRobustness.INDEXING, "" + indexing, CcRobustness.ACQUIRE_READ_LOCKS, "" + grabReadLocks );
                        parameters.putAll( additionalDbConfig );
                        CcInstance instance = new CcProcess().start( parameters );
                        instance.awaitStarted();
                        instance = new CcProcessWrapper( files, consistencyCheck, instance, parameters );
                        return instance;
                    }
                };

        abstract CcInstance start( CcInstanceFiles files, RobustnessConsistencyCheck consistencyCheck, int serverId,
                ReferenceNodeStrategy referenceNodeStrategy, boolean indexing, Map<String,String> additionalDbConfig, LogProvider logProvider,
                boolean acquireReadLocks ) throws Exception;
    }

    public enum InstancePhase
    {
        BRINGING_UP( "U" ),
        UP( "" ),
        SHUTTING_DOWN( "S" ),
        DOWN( "D" );

        private final String shortName;

        InstancePhase( String shortName )
        {
            this.shortName = shortName;
        }

        public String shortName()
        {
            return shortName;
        }
    }

    public class Instance
    {
        private CcInstance ccInstance;
        private InstancePhase phase = InstancePhase.DOWN;
        private int blockedNetwork;

        public CcInstance cc()
        {
            if ( ccInstance == null || phase == InstancePhase.SHUTTING_DOWN )
            {
                return null;
            }
            return ccInstance;
        }

        boolean isLeader() throws RemoteException
        {
            return ccInstance != null && ccInstance.isLeader();
        }

        void bringingUp()
        {
            this.phase = InstancePhase.BRINGING_UP;
        }

        public void up( CcInstance instance )
        {
            this.ccInstance = instance;
            this.phase = InstancePhase.UP;
        }

        void shutDown( ShutdownType type, boolean waitForTransactions ) throws RemoteException
        {
            if ( phase == InstancePhase.BRINGING_UP )
            {
                /*
                 * give instance a chance to start up in a healthy fashion
                 * if it still hasn't started up, it (probably) never will
                 * either way, safe to continue
                 */
                waitForInstanceToComeUp( 2, TimeUnit.MINUTES );
            }

            waitForTransactions( waitForTransactions );

            this.phase = InstancePhase.SHUTTING_DOWN;
            if ( ccInstance != null )
            {
                ccInstance.shutdown( type );
                ccInstance = null;
            }
            this.phase = InstancePhase.DOWN;
        }

        private void waitForTransactions( boolean waitForTransactions )
        {
            try
            {
                if ( waitForTransactions )
                {
                    Thread.sleep( 5000 );
                }
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }

        private void waitForInstanceToComeUp( int ticks, TimeUnit timeUnit )
        {
            long start = nanoTime();

            while ( phase != InstancePhase.UP && (nanoTime() - start) < timeUnit.toNanos( ticks ) )
            {
                try
                {
                    Thread.sleep( 100 );
                }
                catch ( InterruptedException e )
                {
                    // ignore
                }
            }
        }

        void awaitShutDown( int numberOfTicks, TimeUnit timeUnit ) throws InterruptedException
        {
            long start = nanoTime();
            while ( phase != InstancePhase.DOWN && (nanoTime() - start) < timeUnit.toNanos( numberOfTicks ) )
            {
                Thread.sleep( 100 );
            }
            if ( phase != InstancePhase.DOWN )
            {
                throw new IllegalStateException( "Timed out waiting for instance " + this + " to shut down" );
            }
        }

        boolean isShutDown()
        {
            return phase == InstancePhase.DOWN;
        }

        public Restoration blockNetwork( int flags ) throws RemoteException
        {
            final CcInstance ccInstance = this.ccInstance;
            if ( ccInstance != null && ccInstance.blockNetwork( flags ) )
            {
                this.blockedNetwork = flags;
                log( "Blocking network " + networkStatus( new StringBuilder() ) + " for " + ccInstance.getServerId() );
                return () ->
                {
                    try
                    {
                        ccInstance.restoreNetwork();
                        log( "Restored network of " + ccInstance.getServerId() );
                    }
                    catch ( RemoteException e )
                    {
                        // M'kay, yer down anyway
                    }
                    finally
                    {
                        blockedNetwork = 0;
                    }
                };
            }
            return null;
        }

        void augmentStatus( StringBuilder builder ) throws RemoteException
        {
            if ( isLeader() )
            {
                builder.append( "L" );
            }
            if ( blockedNetwork != 0 )
            {
                networkStatus( builder );
            }

            if ( ccInstance != null && !ccInstance.isAvailable() )
            {
                builder.append( "!" );
            }
        }

        private StringBuilder networkStatus( StringBuilder builder )
        {
            builder.append( '(' );
            if ( (blockedNetwork & NF_IN) != 0 )
            {
                builder.append( "-I" );
            }
            if ( (blockedNetwork & NF_OUT) != 0 )
            {
                builder.append( "-O" );
            }
            builder.append( ')' );
            return builder;
        }
    }
}
