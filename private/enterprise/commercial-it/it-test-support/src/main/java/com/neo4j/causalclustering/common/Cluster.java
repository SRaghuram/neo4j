/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.CoreEditionModule;
import com.neo4j.causalclustering.core.CoreGraphDatabase;
import com.neo4j.causalclustering.core.LeaderCanWrite;
import com.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.roles.RoleProvider;
import com.neo4j.causalclustering.core.state.machines.barrier.BarrierState;
import com.neo4j.causalclustering.core.state.machines.id.IdGenerationException;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.IpFamily;
import com.neo4j.causalclustering.discovery.Topology;
import com.neo4j.causalclustering.helper.ErrorHandler;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.causalclustering.readreplica.ReadReplicaEditionModule;
import com.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import com.neo4j.kernel.enterprise.api.security.CommercialSecurityContext;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.function.Predicates;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.lock.AcquireLockTimeoutException;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.ports.PortAuthority;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.function.Predicates.await;
import static org.neo4j.function.Predicates.awaitEx;
import static org.neo4j.function.Predicates.notNull;
import static org.neo4j.internal.helpers.collection.Iterables.firstOrNull;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.LockSessionExpired;
import static org.neo4j.util.concurrent.Futures.combine;

public class Cluster
{
    private static final int DEFAULT_TIMEOUT_MS = (int) MINUTES.toMillis( 3 );
    private static final int DEFAULT_CLUSTER_SIZE = 3;

    private final File parentDir;
    private final Map<String,String> coreParams;
    private final Map<String,IntFunction<String>> instanceCoreParams;
    private final Map<String,String> readReplicaParams;
    private final Map<String,IntFunction<String>> instanceReadReplicaParams;
    private final String recordFormat;
    private final DiscoveryServiceFactory discoveryServiceFactory;
    private final String listenAddress;
    private final String advertisedAddress;

    private final Map<Integer,CoreClusterMember> coreMembers = new ConcurrentHashMap<>();
    private final Map<Integer,ReadReplica> readReplicas = new ConcurrentHashMap<>();
    private int highestCoreServerId;
    private int highestReplicaServerId;

    public Cluster( File parentDir, int noOfCoreMembers, int noOfReadReplicas, DiscoveryServiceFactory discoveryServiceFactory,
            Map<String,String> coreParams,
            Map<String,IntFunction<String>> instanceCoreParams,
            Map<String,String> readReplicaParams, Map<String,IntFunction<String>> instanceReadReplicaParams,
            String recordFormat, IpFamily ipFamily, boolean useWildcard )
    {
        this.discoveryServiceFactory = discoveryServiceFactory;
        this.parentDir = parentDir;
        this.coreParams = coreParams;
        this.instanceCoreParams = instanceCoreParams;
        this.readReplicaParams = readReplicaParams;
        this.instanceReadReplicaParams = instanceReadReplicaParams;
        this.recordFormat = recordFormat;
        listenAddress = useWildcard ? ipFamily.wildcardAddress() : ipFamily.localhostAddress();
        advertisedAddress = ipFamily.localhostName();
        List<SocketAddress> initialHosts = initialHosts( noOfCoreMembers );
        createCoreMembers( noOfCoreMembers, initialHosts, coreParams, instanceCoreParams, recordFormat );
        createReadReplicas( noOfReadReplicas, initialHosts, readReplicaParams, instanceReadReplicaParams, recordFormat );
    }

    private List<SocketAddress> initialHosts( int noOfCoreMembers )
    {
        return IntStream.range( 0, noOfCoreMembers )
                .mapToObj( ignored -> PortAuthority.allocatePort() )
                .map( port -> new SocketAddress( advertisedAddress, port ) )
                .collect( toList() );
    }

    public void start() throws InterruptedException, ExecutionException
    {
        startCoreMembers();
        startReadReplicas();
    }

    public Set<CoreClusterMember> healthyCoreMembers()
    {
        return coreMembers.values().stream()
                .filter( db -> db.defaultDatabase().getDependencyResolver().resolveDependency( DatabaseHealth.class ).isHealthy() )
                .collect( Collectors.toSet() );
    }

    public CoreClusterMember getCoreMemberById( int memberId )
    {
        return coreMembers.get( memberId );
    }

    public ReadReplica getReadReplicaById( int memberId )
    {
        return readReplicas.get( memberId );
    }

    public CoreClusterMember addCoreMemberWithId( int memberId )
    {
        return addCoreMemberWithId( memberId, coreParams, instanceCoreParams, recordFormat );
    }

    public CoreClusterMember newCoreMember()
    {
        int newCoreServerId = ++highestCoreServerId;
        return addCoreMemberWithId( newCoreServerId );
    }

    public ReadReplica newReadReplica()
    {
        int newReplicaServerId = ++highestReplicaServerId;
        return addReadReplicaWithId( newReplicaServerId );
    }

    private CoreClusterMember addCoreMemberWithId( int memberId, Map<String,String> extraParams, Map<String,IntFunction<String>> instanceExtraParams,
            String recordFormat )
    {
        List<SocketAddress> initialHosts = extractInitialHosts( coreMembers );
        CoreClusterMember coreClusterMember = createCoreClusterMember( memberId, PortAuthority.allocatePort(), DEFAULT_CLUSTER_SIZE, initialHosts, recordFormat,
                extraParams, instanceExtraParams );

        coreMembers.put( memberId, coreClusterMember );
        return coreClusterMember;
    }

    public static void startMembers( ClusterMember... clusterMembers ) throws ExecutionException, InterruptedException
    {
        startMembers( Arrays.asList( clusterMembers ) );
    }

    public static void startMembers( Collection<? extends ClusterMember> clusterMembers ) throws ExecutionException, InterruptedException
    {
        combine( invokeAll( "starting-members", clusterMembers, cm ->
        {
            cm.start();
            return null;
        } ) ).get();
    }

    public ReadReplica addReadReplicaWithIdAndRecordFormat( int memberId, String recordFormat )
    {
        return addReadReplica( memberId, recordFormat, new Monitors() );
    }

    public ReadReplica addReadReplicaWithId( int memberId )
    {
        return addReadReplicaWithIdAndRecordFormat( memberId, recordFormat );
    }

    public ReadReplica addReadReplicaWithIdAndMonitors( @SuppressWarnings( "SameParameterValue" ) int memberId, Monitors monitors )
    {
        return addReadReplica( memberId, recordFormat, monitors );
    }

    private ReadReplica addReadReplica( int memberId, String recordFormat, Monitors monitors )
    {
        List<SocketAddress> initialHosts = extractInitialHosts( coreMembers );
        ReadReplica member = createReadReplica( memberId, initialHosts, readReplicaParams, instanceReadReplicaParams, recordFormat, monitors );

        readReplicas.put( memberId, member );
        return member;
    }

    public void shutdown()
    {
        try ( ErrorHandler errorHandler = new ErrorHandler( "Error when trying to shutdown cluster" ) )
        {
            shutdownMembers( coreMembers(), errorHandler );
            shutdownMembers( readReplicas(), errorHandler );
        }
    }

    public void shutdownCoreMembers()
    {
        shutdownMembers( coreMembers() );
    }

    public void shutdownReadReplicas()
    {
        shutdownMembers( readReplicas() );
    }

    public static void shutdownMembers( ClusterMember... clusterMembers )
    {
        shutdownMembers( Arrays.asList( clusterMembers ) );
    }

    public static void shutdownMembers( Collection<? extends ClusterMember> clusterMembers )
    {
        try ( ErrorHandler errorHandler = new ErrorHandler( "Error when trying to shutdown members" ) )
        {
            shutdownMembers( clusterMembers, errorHandler );
        }
    }

    private static void shutdownMembers( Collection<? extends ClusterMember> clusterMembers, ErrorHandler errorHandler )
    {
        errorHandler.execute( () -> combine( invokeAll( "cluster-shutdown", clusterMembers, cm ->
        {
            cm.shutdown();
            return null;
        } ) ).get() );
    }

    private static <X extends GraphDatabaseAPI, T extends ClusterMember, R> List<Future<R>> invokeAll( String threadName, Collection<T> members,
            Function<T,R> call )
    {
        List<Future<R>> list = new ArrayList<>( members.size() );
        int threadNumber = 0;
        for ( T member : members )
        {
            FutureTask<R> task = new FutureTask<>( () -> call.apply( member ) );
            ThreadGroup threadGroup = member.threadGroup();
            Thread thread = new Thread( threadGroup, task, threadName + "-" + threadNumber );
            thread.start();
            threadNumber++;
            list.add( task );
        }
        return list;
    }

    public void removeCoreMembers( Collection<CoreClusterMember> coreClusterMembers )
    {
        shutdownMembers( coreClusterMembers );
        coreMembers.values().removeAll( coreClusterMembers );
    }

    public void removeReadReplicas( Collection<ReadReplica> readReplicasToRemove )
    {
        shutdownMembers( readReplicasToRemove );
        readReplicas.values().removeAll( readReplicasToRemove );
    }

    public void removeCoreMemberWithServerId( int serverId )
    {
        CoreClusterMember memberToRemove = getCoreMemberById( serverId );

        if ( memberToRemove != null )
        {
            memberToRemove.shutdown();
            removeCoreMember( memberToRemove );
        }
        else
        {
            throw new RuntimeException( "Could not remove core member with id " + serverId );
        }
    }

    public void removeCoreMember( CoreClusterMember memberToRemove )
    {
        memberToRemove.shutdown();
        coreMembers.values().remove( memberToRemove );
    }

    public void removeReadReplicaWithMemberId( int memberId )
    {
        ReadReplica memberToRemove = getReadReplicaById( memberId );

        if ( memberToRemove != null )
        {
            removeReadReplica( memberToRemove );
        }
        else
        {
            throw new RuntimeException( "Could not remove core member with member id " + memberId );
        }
    }

    public void removeReadReplica( ReadReplica memberToRemove )
    {
        memberToRemove.shutdown();
        readReplicas.values().remove( memberToRemove );
    }

    public Collection<CoreClusterMember> coreMembers()
    {
        return coreMembers.values();
    }

    public Collection<ReadReplica> readReplicas()
    {
        return readReplicas.values();
    }

    public ReadReplica findAnyReadReplica()
    {
        return firstOrNull( readReplicas.values() );
    }

    private CoreClusterMember getMemberWithRole( Role role )
    {
        return getMemberWithAnyRole( role );
    }

    public List<CoreClusterMember> getAllMembersWithRole( Role role )
    {
        return getAllMembersWithAnyRole( role );
    }

    private CoreClusterMember getMemberWithRole( String databaseName, Role role )
    {
        return getMemberWithAnyRole( databaseName, role );
    }

    public CoreClusterMember getMemberWithAnyRole( Role... roles )
    {
        return getMemberWithAnyRole( DEFAULT_DATABASE_NAME, roles );
    }

    private List<CoreClusterMember> getAllMembersWithAnyRole( Role... roles )
    {
        return getAllMembersWithAnyRole( DEFAULT_DATABASE_NAME, roles );
    }

    public CoreClusterMember getMemberWithAnyRole( String databaseName, Role... roles )
    {
        return getAllMembersWithAnyRole( databaseName, roles ).stream().findFirst().orElse( null );
    }

    private List<CoreClusterMember> getAllMembersWithAnyRole( String databaseName, Role... roles )
    {
        var roleSet = Arrays.stream( roles ).collect( toSet() );

        var list = new ArrayList<CoreClusterMember>();
        for ( CoreClusterMember m : coreMembers.values() )
        {
            var managementService = m.managementService();
            if ( managementService == null )
            {
                continue;
            }

            try
            {
                var database = (GraphDatabaseFacade) managementService.database( databaseName );
                if ( roleSet.contains( getCurrentDatabaseRole( database ) ) )
                {
                    list.add( m );
                }
            }
            catch ( DatabaseNotFoundException e )
            {
                // ignored
            }
        }
        return list;
    }

    private static Role getCurrentDatabaseRole( GraphDatabaseFacade database )
    {
        return database.getDependencyResolver().resolveDependency( RoleProvider.class ).currentRole();
    }

    public CoreClusterMember awaitLeader() throws TimeoutException
    {
        return awaitCoreMemberWithRole( Role.LEADER, DEFAULT_TIMEOUT_MS, MILLISECONDS );
    }

    public CoreClusterMember awaitLeader( String databaseName ) throws TimeoutException
    {
        return awaitCoreMemberWithRole( databaseName, Role.LEADER, DEFAULT_TIMEOUT_MS, MILLISECONDS );
    }

    public CoreClusterMember awaitLeader( String databaseName, long timeout, TimeUnit timeUnit ) throws TimeoutException
    {
        return awaitCoreMemberWithRole( databaseName, Role.LEADER, timeout, timeUnit );
    }

    public CoreClusterMember awaitLeader( long timeout, TimeUnit timeUnit ) throws TimeoutException
    {
        return awaitCoreMemberWithRole( Role.LEADER, timeout, timeUnit );
    }

    public CoreClusterMember awaitCoreMemberWithRole( Role role ) throws TimeoutException
    {
        return awaitCoreMemberWithRole( role, DEFAULT_TIMEOUT_MS, MILLISECONDS );
    }

    public CoreClusterMember awaitCoreMemberWithRole( Role role, long timeout, TimeUnit timeUnit ) throws TimeoutException
    {
        return await( () -> getMemberWithRole( role ), notNull(), timeout, timeUnit );
    }

    @SuppressWarnings( "SameParameterValue" )
    private CoreClusterMember awaitCoreMemberWithRole( String databaseName, Role role, long timeout, TimeUnit timeUnit ) throws TimeoutException
    {
        return await( () -> getMemberWithRole( databaseName, role ), notNull(), timeout, timeUnit );
    }

    public int numberOfCoreMembersReportedByTopology( String databaseName )
    {
        return numberOfMembersReportedByCoreTopology( databaseName, CoreTopologyService::coreTopologyForDatabase );
    }

    public int numberOfReadReplicaMembersReportedByTopology( String databaseName )
    {
        return numberOfMembersReportedByCoreTopology( databaseName, CoreTopologyService::readReplicaTopologyForDatabase );
    }

    private int numberOfMembersReportedByCoreTopology( String databaseName, BiFunction<CoreTopologyService,DatabaseId,Topology<?>> topologySelector )
    {
        return coreMembers
                .values()
                .stream()
                .filter( member -> member.defaultDatabase() != null )
                .filter( member -> !member.hasPanicked() )
                .findAny()
                .map( coreClusterMember ->
                {
                    var coreTopologyService = coreClusterMember.defaultDatabase().getDependencyResolver().resolveDependency( CoreTopologyService.class );
                    var databaseId = coreClusterMember.databaseIdRepository().get( databaseName );
                    return topologySelector.apply( coreTopologyService, databaseId ).members().size();
                } )
                .orElse( 0 );
    }

    /**
     * Perform a transaction against the core cluster, selecting the target and retrying as necessary.
     */
    public CoreClusterMember coreTx( BiConsumer<GraphDatabaseFacade,Transaction> op ) throws Exception
    {
        return coreTx( DEFAULT_DATABASE_NAME, op );
    }

    /**
     * Perform a transaction against the core cluster, selecting the target and retrying as necessary.
     */
    public CoreClusterMember systemTx( BiConsumer<GraphDatabaseFacade,Transaction> op ) throws Exception
    {
        return coreTx( SYSTEM_DATABASE_NAME, op );
    }

    /**
     * Perform a transaction against the core cluster, selecting the target and retrying as necessary.
     */
    public CoreClusterMember coreTx( String databaseName, BiConsumer<GraphDatabaseFacade,Transaction> op ) throws Exception
    {
        return leaderTx( databaseName, op, DEFAULT_TIMEOUT_MS, MILLISECONDS );
    }

    /**
     * Perform a transaction against the leader of the core cluster, retrying as necessary.
     */
    @SuppressWarnings( "SameParameterValue" )
    private CoreClusterMember leaderTx( String databaseName, BiConsumer<GraphDatabaseFacade,Transaction> op, int timeout, TimeUnit timeUnit )
            throws Exception
    {
        ThrowingSupplier<CoreClusterMember,Exception> supplier = () ->
        {
            CoreClusterMember member = awaitLeader( databaseName, timeout, timeUnit );
            GraphDatabaseFacade db = (GraphDatabaseFacade) member.managementService().database( databaseName );
            if ( db == null )
            {
                throw new DatabaseShutdownException();
            }

            try ( Transaction tx = db.beginTransaction( KernelTransaction.Type.explicit, CommercialSecurityContext.AUTH_DISABLED ) )
            {
                op.accept( db, tx );
                return member;
            }
            catch ( Throwable e )
            {
                if ( isTransientFailure( e ) )
                {
                    return null;
                }
                else
                {
                    throw e;
                }
            }
        };
        return awaitEx( supplier, notNull()::test, timeout, timeUnit );
    }

    private static boolean isTransientFailure( Throwable e )
    {
        Predicate<Throwable> throwablePredicate =
                e1 -> isLockExpired( e1 ) || isLockOnFollower( e1 ) || isWriteNotOnLeader( e1 ) || e1 instanceof TransientTransactionFailureException ||
                        e1 instanceof IdGenerationException;
        return Exceptions.contains( e, throwablePredicate );
    }

    private static boolean isWriteNotOnLeader( Throwable e )
    {
        return e instanceof WriteOperationsNotAllowedException &&
               e.getMessage().startsWith( String.format( LeaderCanWrite.NOT_LEADER_ERROR_MSG, "" ) );
    }

    private static boolean isLockOnFollower( Throwable e )
    {
        return e instanceof AcquireLockTimeoutException &&
               (e.getMessage().equals( BarrierState.TOKEN_NOT_ON_LEADER_ERROR_MESSAGE ) ||
                e.getCause() instanceof NoLeaderFoundException);
    }

    private static boolean isLockExpired( Throwable e )
    {
        return e instanceof TransactionFailureException &&
               e.getCause() instanceof org.neo4j.internal.kernel.api.exceptions.TransactionFailureException &&
               ((org.neo4j.internal.kernel.api.exceptions.TransactionFailureException) e.getCause()).status() ==
               LockSessionExpired;
    }

    private List<SocketAddress> extractInitialHosts( Map<Integer,CoreClusterMember> coreMembers )
    {
        return coreMembers.values().stream()
                .map( CoreClusterMember::discoveryPort )
                .map( port -> new SocketAddress( advertisedAddress, port ) )
                .collect( toList() );
    }

    private void createCoreMembers( final int noOfCoreMembers,
            List<SocketAddress> initialHosts, Map<String,String> extraParams,
            Map<String,IntFunction<String>> instanceExtraParams, String recordFormat )
    {
        for ( int i = 0; i < initialHosts.size(); i++ )
        {
            int discoveryListenAddress = initialHosts.get( i ).getPort();
            CoreClusterMember coreClusterMember = createCoreClusterMember(
                    i,
                    discoveryListenAddress,
                    noOfCoreMembers,
                    initialHosts,
                    recordFormat,
                    extraParams,
                    instanceExtraParams
            );
            coreMembers.put( i, coreClusterMember );
        }
        highestCoreServerId = noOfCoreMembers - 1;
    }

    private CoreClusterMember createCoreClusterMember( int serverId,
            int discoveryPort,
            int clusterSize,
            List<SocketAddress> initialHosts,
            String recordFormat,
            Map<String,String> extraParams,
            Map<String,IntFunction<String>> instanceExtraParams )
    {
        int txPort = PortAuthority.allocatePort();
        int raftPort = PortAuthority.allocatePort();
        int boltPort = PortAuthority.allocatePort();
        int httpPort = PortAuthority.allocatePort();
        int backupPort = PortAuthority.allocatePort();

        return new CoreClusterMember(
                serverId,
                discoveryPort,
                txPort,
                raftPort,
                boltPort,
                httpPort,
                backupPort,
                clusterSize,
                initialHosts,
                discoveryServiceFactory,
                recordFormat,
                parentDir,
                extraParams,
                instanceExtraParams,
                listenAddress,
                advertisedAddress,
                ( File file, Config config, GraphDatabaseDependencies dependencies, DiscoveryServiceFactory discoveryServiceFactory ) ->
                        new CoreGraphDatabase( file, config, dependencies, discoveryServiceFactory, CoreEditionModule::new )
        );
    }

    private ReadReplica createReadReplica( int serverId,
            List<SocketAddress> initialHosts,
            Map<String,String> extraParams,
            Map<String,IntFunction<String>> instanceExtraParams,
            String recordFormat,
            Monitors monitors )
    {
        int boltPort = PortAuthority.allocatePort();
        int httpPort = PortAuthority.allocatePort();
        int txPort = PortAuthority.allocatePort();
        int backupPort = PortAuthority.allocatePort();
        int discoveryPort = PortAuthority.allocatePort();

        return new ReadReplica(
                parentDir,
                serverId,
                boltPort,
                httpPort,
                txPort,
                backupPort,
                discoveryPort,
                discoveryServiceFactory,
                initialHosts,
                extraParams,
                instanceExtraParams,
                recordFormat,
                monitors,
                advertisedAddress,
                listenAddress,
                ( File file, Config config, GraphDatabaseDependencies dependencies, DiscoveryServiceFactory discoveryServiceFactory, MemberId memberId ) ->
                        new ReadReplicaGraphDatabase( file, config, dependencies, discoveryServiceFactory, memberId, ReadReplicaEditionModule::new )
        );
    }

    public void startCoreMembers() throws InterruptedException, ExecutionException
    {
        startMembers( coreMembers() );
    }

    private void startReadReplicas() throws InterruptedException, ExecutionException
    {
        startMembers( readReplicas() );
    }

    private void createReadReplicas( int noOfReadReplicas,
            final List<SocketAddress> initialHosts,
            Map<String,String> extraParams,
            Map<String,IntFunction<String>> instanceExtraParams,
            String recordFormat )
    {
        for ( int i = 0; i < noOfReadReplicas; i++ )
        {
            ReadReplica readReplica = createReadReplica(
                    i,
                    initialHosts,
                    extraParams,
                    instanceExtraParams,
                    recordFormat,
                    new Monitors()
            );

            readReplicas.put( i, readReplica );
        }
        highestReplicaServerId = noOfReadReplicas - 1;
    }

    /**
     * Awaits for all given members to eventually have database content equivalent to the given other cluster member.
     * Timeout of {@link #DEFAULT_TIMEOUT_MS} milliseconds is used.
     *
     * @param expectedMember cluster member to match database representation with.
     * @param targets cluster members to check.
     */
    public static void dataMatchesEventually( ClusterMember expectedMember, Collection<? extends ClusterMember> targets ) throws TimeoutException
    {
        dataMatchesEventually( () -> buildDbRepresentation( expectedMember ), targets );
    }

    /**
     * Awaits for all given members to eventually have database content equivalent to the given {@link DbRepresentation}.
     * Timeout of {@link #DEFAULT_TIMEOUT_MS} milliseconds is used.
     *
     * @param expected expected database representation.
     * @param targets cluster members to check.
     */
    public static void dataMatchesEventually( DbRepresentation expected, Collection<? extends ClusterMember> targets ) throws TimeoutException
    {
        dataMatchesEventually( () -> expected, targets );
    }

    public Optional<ClusterMember> randomMember( boolean mustBeStarted )
    {
        Stream<ClusterMember> members = Stream.concat( coreMembers().stream(), readReplicas().stream() );

        if ( mustBeStarted )
        {
            members = members.filter( m -> !m.isShutdown() );
        }

        List<ClusterMember> eligible = members.collect( Collectors.toList() );
        return random( eligible );
    }

    public Optional<CoreClusterMember> randomCoreMember( boolean mustBeStarted )
    {
        Stream<CoreClusterMember> members = coreMembers().stream();

        if ( mustBeStarted )
        {
            members = members.filter( m -> !m.isShutdown() );
        }

        List<CoreClusterMember> eligible = members.collect( Collectors.toList() );
        return random( eligible );
    }

    private static <T> Optional<T> random( List<T> list )
    {
        if ( list.isEmpty() )
        {
            return Optional.empty();
        }
        int ordinal = ThreadLocalRandom.current().nextInt( list.size() );
        return Optional.of( list.get( ordinal ) );
    }

    public Stream<ClusterMember> allMembers()
    {
        return Stream.concat( coreMembers.values().stream(), readReplicas.values().stream() );
    }

    private static void dataMatchesEventually( Supplier<DbRepresentation> expected, Collection<? extends ClusterMember> members ) throws TimeoutException
    {
        for ( ClusterMember member : members )
        {
            Predicates.await( () -> Objects.equals( expected.get(), buildDbRepresentation( member ) ), DEFAULT_TIMEOUT_MS, MILLISECONDS );
        }
    }

    private static DbRepresentation buildDbRepresentation( ClusterMember member )
    {
        try
        {
            var db = member.defaultDatabase();
            if ( db == null )
            {
                // cluster member is shutdown
                return null;
            }
            return DbRepresentation.of( db );
        }
        catch ( DatabaseShutdownException e )
        {
            // this can happen if the database is still in the process of starting or doing a store copy
            return null;
        }
    }
}
