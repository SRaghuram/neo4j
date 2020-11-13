/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.roles.RoleProvider;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.IpFamily;
import com.neo4j.causalclustering.discovery.Topology;
import com.neo4j.causalclustering.helper.ErrorHandler;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.causalclustering.read_replica.TestReadReplicaGraphDatabase;
import com.neo4j.causalclustering.readreplica.ReadReplicaEditionModule;
import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.driver.net.ServerAddress;
import org.neo4j.driver.net.ServerAddressResolver;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.ports.PortAuthority;

import static java.util.Collections.shuffle;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_advertised_address;
import static org.neo4j.configuration.GraphDatabaseSettings.default_listen_address;
import static org.neo4j.function.Predicates.await;
import static org.neo4j.function.Predicates.awaitEx;
import static org.neo4j.function.Predicates.notNull;
import static org.neo4j.internal.helpers.collection.Iterables.firstOrNull;
import static org.neo4j.kernel.api.security.AuthToken.newBasicAuthToken;
import static org.neo4j.util.concurrent.Futures.combine;

public class Cluster implements ServerAddressResolver
{
    private static final int DEFAULT_TIMEOUT_MS = (int) MINUTES.toMillis( 3 );
    private static final int DEFAULT_CLUSTER_SIZE = 3;

    public static final Duration TOPOLOGY_REFRESH_INTERVAL = Duration.ofSeconds( 1 );

    private final Path parentDir;
    private final Map<String,String> coreParams;
    private final Map<String,IntFunction<String>> instanceCoreParams;
    private final Map<String,String> readReplicaParams;
    private final Map<String,IntFunction<String>> instanceReadReplicaParams;
    private final String recordFormat;
    private final DiscoveryServiceFactory discoveryServiceFactory;
    private final String defaultListenHost;
    private final String defaultAdvertisedHost;

    private final Map<Integer,CoreClusterMember> coreMembers = new ConcurrentHashMap<>();
    private final Map<Integer,ReadReplica> readReplicas = new ConcurrentHashMap<>();
    private int highestCoreIndex;
    private int highestReplicaIndex;

    public Cluster( Path parentDir, int noOfCoreMembers, int noOfReadReplicas, DiscoveryServiceFactory discoveryServiceFactory,
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
        defaultListenHost = useWildcard ? ipFamily.wildcardAddress() : ipFamily.localhostAddress();
        defaultAdvertisedHost = ipFamily.localhostName();
        var initialHosts = initialHosts( noOfCoreMembers, coreParams, instanceCoreParams );
        createCoreMembers( noOfCoreMembers, initialHosts, coreParams, instanceCoreParams, recordFormat );
        createReadReplicas( noOfReadReplicas, initialHosts, readReplicaParams, instanceReadReplicaParams, recordFormat );
    }

    private List<SocketAddress> initialHosts( int noOfCoreMembers, Map<String,String> coreParams, Map<String,IntFunction<String>> instanceCoreParams )
    {
        return IntStream.range( 0, noOfCoreMembers )
                .mapToObj( index -> getAdvertisedHost( index, coreParams, instanceCoreParams ) )
                .map( advertisedHost -> new SocketAddress( advertisedHost, PortAuthority.allocatePort() ) )
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

    public CoreClusterMember getCoreMemberByIndex( int index )
    {
        return coreMembers.get( index );
    }

    public ReadReplica getReadReplicaByIndex( int index )
    {
        return readReplicas.get( index );
    }

    public CoreClusterMember addCoreMemberWithIndex( int index )
    {
        return addCoreMemberWithIndex( index, coreParams, instanceCoreParams, recordFormat );
    }

    public CoreClusterMember newCoreMember()
    {
        var newCoreServerIndex = ++highestCoreIndex;
        return addCoreMemberWithIndex( newCoreServerIndex );
    }

    public ReadReplica newReadReplica()
    {
        var newReplicaServerIndex = ++highestReplicaIndex;
        return addReadReplicaWithIndex( newReplicaServerIndex );
    }

    private CoreClusterMember addCoreMemberWithIndex( int index, Map<String,String> extraParams, Map<String,IntFunction<String>> instanceExtraParams,
            String recordFormat )
    {
        List<SocketAddress> initialHosts = extractInitialHosts( coreMembers );
        CoreClusterMember coreClusterMember = createCoreClusterMember( index, PortAuthority.allocatePort(), DEFAULT_CLUSTER_SIZE, initialHosts, recordFormat,
                extraParams, instanceExtraParams );

        coreMembers.put( index, coreClusterMember );
        return coreClusterMember;
    }

    public static void startMembers( ClusterMember... clusterMembers ) throws ExecutionException, InterruptedException
    {
        startMembers( Arrays.asList( clusterMembers ) );
    }

    private static void startMembers( Collection<? extends ClusterMember> clusterMembers ) throws ExecutionException, InterruptedException
    {
        combine( invokeAll( "starting-members", clusterMembers, cm ->
        {
            cm.start();
            return null;
        } ) ).get();
    }

    public ReadReplica addReadReplicaWithIndexAndRecordFormat( int index, String recordFormat )
    {
        return addReadReplica( index, recordFormat, new Monitors() );
    }

    public ReadReplica addReadReplicaWithIndex( int index )
    {
        return addReadReplicaWithIndexAndRecordFormat( index, recordFormat );
    }

    public ReadReplica addReadReplicaWithIndexAndMonitors( @SuppressWarnings( "SameParameterValue" ) int index, Monitors monitors )
    {
        return addReadReplica( index, recordFormat, monitors );
    }

    private ReadReplica addReadReplica( int index, String recordFormat, Monitors monitors )
    {
        List<SocketAddress> initialHosts = extractInitialHosts( coreMembers );
        ReadReplica member = createReadReplica( index, initialHosts, readReplicaParams, instanceReadReplicaParams, recordFormat, monitors );

        readReplicas.put( index, member );
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

    private static void shutdownMembers( Collection<? extends ClusterMember> clusterMembers )
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

    private static <T extends ClusterMember, R> List<Future<R>> invokeAll( String threadName, Collection<T> members, Function<T,R> call )
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

    public void removeCoreMemberWithIndex( int index )
    {
        CoreClusterMember memberToRemove = getCoreMemberByIndex( index );

        if ( memberToRemove != null )
        {
            memberToRemove.shutdown();
            removeCoreMember( memberToRemove );
        }
        else
        {
            throw new RuntimeException( "Could not remove core member with index " + index );
        }
    }

    public void removeCoreMember( CoreClusterMember memberToRemove )
    {
        shutdownMembers( memberToRemove );
        coreMembers.values().remove( memberToRemove );
    }

    public void removeReadReplicaWithIndex( int index )
    {
        ReadReplica memberToRemove = getReadReplicaByIndex( index );

        if ( memberToRemove != null )
        {
            removeReadReplica( memberToRemove );
        }
        else
        {
            throw new RuntimeException( "Could not remove core member with member index " + index );
        }
    }

    public void removeReadReplica( ReadReplica memberToRemove )
    {
        memberToRemove.shutdown();
        readReplicas.values().remove( memberToRemove );
    }

    public Set<CoreClusterMember> coreMembers()
    {
        return Set.copyOf( coreMembers.values() );
    }

    public Set<ReadReplica> readReplicas()
    {
        return Set.copyOf( readReplicas.values() );
    }

    public Set<ClusterMember> allMembers()
    {
        var result = new HashSet<ClusterMember>();
        result.addAll( coreMembers.values() );
        result.addAll( readReplicas.values() );
        return result;
    }

    public ReadReplica findAnyReadReplica()
    {
        return firstOrNull( readReplicas.values() );
    }

    public List<CoreClusterMember> getAllMembersWithRole( Role role )
    {
        return getAllMembersWithRole( DEFAULT_DATABASE_NAME, role );
    }

    public List<CoreClusterMember> getAllMembersWithRole( String databaseName, Role role )
    {
        return getAllActiveMembersWithAnyRole( databaseName, role );
    }

    /**
     * Return a randomly selected member that matches any of the provided roles.
     * N.b. Use {@link #awaitCoreMemberWithRole} to assert that a member is returned and allow time for cluster changes such as elections }
     * @param roles
     * @return null if no members are found matching any of the provided roles.
     */
    public CoreClusterMember getMemberWithAnyRole( Role... roles )
    {
        return getAnyActiveMemberWithAnyRole( DEFAULT_DATABASE_NAME, roles ).orElse( null );
    }

    public CoreClusterMember getMemberWithAnyRole( String databaseName, Role... roles )
    {
        return getAnyActiveMemberWithAnyRole( databaseName, roles ).orElse( null );
    }

    private Optional<CoreClusterMember> getAnyActiveMemberWithAnyRole( String databaseName, Role... roles )
    {
        return getAllActiveMembersWithAnyRole( databaseName, roles ).stream().findFirst();
    }

    private List<CoreClusterMember> getAllActiveMembersWithAnyRole( String databaseName, Role... roles )
    {
        return getAllMembersWithAnyRole( databaseName, roles ).stream().filter( member -> {
            try
            {
                var managementService = member.managementService();
                return managementService != null &&
                       managementService.database( databaseName ).isAvailable( -1 );
            }
            catch ( DatabaseNotFoundException e )
            {
                return false;
            }
        } ).collect( Collectors.toList() );
    }

    private List<CoreClusterMember> getAllMembersWithAnyRole( String databaseName, Role... roles )
    {
        var roleSet = Arrays.stream( roles ).collect( toSet() );

        var list = new ArrayList<CoreClusterMember>();
        for ( CoreClusterMember member : coreMembers.values() )
        {
            var managementService = member.managementService();
            if ( managementService == null )
            {
                continue;
            }

            try
            {
                var database = (GraphDatabaseFacade) managementService.database( databaseName );
                if ( roleSet.contains( getCurrentDatabaseRole( database ) ) )
                {
                    list.add( member );
                }
            }
            catch ( DatabaseNotFoundException e )
            {
                // ignored
            }
        }

        // Shuffle the list to avoid implicit ordering-related problems
        Collections.shuffle( list );
        return list;
    }

    private static Role getCurrentDatabaseRole( GraphDatabaseFacade database )
    {
        DependencyResolver dependencyResolver = database.getDependencyResolver();
        if ( dependencyResolver == null )
        {
            return null;
        }
        return dependencyResolver.resolveDependency( RoleProvider.class ).currentRole();
    }

    public boolean isCoreLeader( CoreClusterMember core, String databaseName )
    {
        return getCurrentDatabaseRole( core.database( databaseName ) ) == Role.LEADER;
    }

    public CoreClusterMember awaitLeader() throws TimeoutException
    {
        return awaitLeader( DEFAULT_DATABASE_NAME );
    }

    public CoreClusterMember awaitLeader( String databaseName ) throws TimeoutException
    {
        return awaitLeader( databaseName, DEFAULT_TIMEOUT_MS, MILLISECONDS );
    }

    public CoreClusterMember awaitLeader( String databaseName, long timeout, TimeUnit timeUnit ) throws TimeoutException
    {
        return awaitCoreMemberWithRole( databaseName, Role.LEADER, timeout, timeUnit );
    }

    public CoreClusterMember awaitCoreMemberWithRole( Role role ) throws TimeoutException
    {
        return awaitCoreMemberWithRole( DEFAULT_DATABASE_NAME, role );
    }

    public CoreClusterMember awaitCoreMemberWithRole( String databaseName, Role role ) throws TimeoutException
    {
        return awaitCoreMemberWithRole( databaseName, role, DEFAULT_TIMEOUT_MS, MILLISECONDS );
    }

    public CoreClusterMember awaitCoreMemberWithRole( String databaseName, Role role, long timeout, TimeUnit timeUnit ) throws TimeoutException
    {
        return awaitCoreMemberWithAnyRole( databaseName, timeout, timeUnit, role );
    }

    @SuppressWarnings( "SameParameterValue" )
    private CoreClusterMember awaitCoreMemberWithAnyRole( String databaseName, long timeout, TimeUnit timeUnit, Role... roles ) throws TimeoutException
    {
        return await( () -> getAnyActiveMemberWithAnyRole( databaseName, roles ).orElse( null ), notNull(), timeout, timeUnit );
    }

    public void awaitAllCoresJoinedAllRaftGroups( Set<String> databases, long timeout, TimeUnit timeUnit ) throws TimeoutException
    {
        await( () -> coreMembers.values().stream().allMatch( core -> databases.stream().allMatch(
               databaseName -> core.resolveDependency( databaseName, RaftMachine.class ).votingMembers().containsAll(
                        coreMembers.values().stream().map( member -> member.raftMemberIdFor( member.databaseId( databaseName ) ) )
                                .collect( Collectors.toSet() ) ) ) ),
               timeout,
               timeUnit
        );
    }

    public int numberOfCoreMembersReportedByTopology( String databaseName )
    {
        return numberOfMembersReportedByCoreTopology( databaseName, CoreTopologyService::coreTopologyForDatabase );
    }

    public List<Integer> numberOfCoreMembersReportedByTopologyOnAllCores( String databaseName )
    {
        return numberOfMembersReportedByCoreTopologyOnAllCores( databaseName, CoreTopologyService::coreTopologyForDatabase ).collect(toList());
    }

    public int numberOfReadReplicaMembersReportedByTopology( String databaseName )
    {
        return numberOfMembersReportedByCoreTopology( databaseName, CoreTopologyService::readReplicaTopologyForDatabase );
    }

    private int numberOfMembersReportedByCoreTopology( String databaseName, BiFunction<CoreTopologyService,NamedDatabaseId,Topology<?>> topologySelector )
    {
        return numberOfMembersReportedByCoreTopologyOnAllCores(databaseName, topologySelector)
                .findAny()
                .orElse( 0 );
    }

    private Stream<Integer> numberOfMembersReportedByCoreTopologyOnAllCores(
            String databaseName,
            BiFunction<CoreTopologyService,NamedDatabaseId,Topology<?>> topologySelector )
    {
        return coreMembers
                .values()
                .stream()
                .filter( c -> c.managementService() != null && c.database( databaseName ) != null )
                .map( coreClusterMember ->
                      {
                          var db = coreClusterMember.database( databaseName );
                          var coreTopologyService = db.getDependencyResolver().resolveDependency( CoreTopologyService.class );
                          var databaseId = coreClusterMember.databaseId( databaseName );
                          return topologySelector.apply( coreTopologyService, databaseId ).servers().size();
                      }
                );
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
    public void systemTx( BiConsumer<GraphDatabaseFacade,Transaction> op ) throws Exception
    {
        coreTx( SYSTEM_DATABASE_NAME, op );
    }

    /**
     * Perform a transaction against the core cluster, selecting the target and retrying as necessary.
     */
    public CoreClusterMember coreTx( String databaseName, BiConsumer<GraphDatabaseFacade,Transaction> op ) throws Exception
    {
        return coreTx( databaseName, op, DEFAULT_TIMEOUT_MS, MILLISECONDS );
    }

    /**
     * Perform a transaction against the leader of the core cluster, retrying as necessary.
     */
    @SuppressWarnings( "SameParameterValue" )
    public CoreClusterMember coreTx( String databaseName, BiConsumer<GraphDatabaseFacade,Transaction> op, int timeout, TimeUnit timeUnit )
            throws Exception
    {
      return coreTx( databaseName, Role.LEADER, op, timeout, timeUnit );
    }

    /**
     * Perform a transaction against a member with given role of the core cluster, retrying as necessary.
     */
    @SuppressWarnings( "SameParameterValue" )
    public CoreClusterMember coreTx( String databaseName, Role role, BiConsumer<GraphDatabaseFacade,Transaction> op, int timeout, TimeUnit timeUnit )
            throws Exception
    {
        return coreTx( databaseName, role, op, timeout, timeUnit,  x -> EnterpriseSecurityContext.AUTH_DISABLED );
    }

    /**
     * Perform a transaction against a member with given role of the core cluster, as the specified user, retrying as necessary.
     */
    @SuppressWarnings( "SameParameterValue" )
    public CoreClusterMember coreTx( String databaseName, Role role, BiConsumer<GraphDatabaseFacade,Transaction> op, int timeout, TimeUnit timeUnit,
                                     String username, String password )
            throws Exception
    {
        return coreTx( databaseName, role, op, timeout, timeUnit, db -> {
            var authManager = db.getDependencyResolver().resolveDependency( EnterpriseAuthManager.class);
            return authManager.login(newBasicAuthToken(username, password));
        });
    }

    /**
     * Perform a transaction against a member with given role of the core cluster, retrying as necessary.
     *
     * Transactions may only successfully be committed against the leader of a cluster,
     * but it is useful to be able to attempt transactions against other members,
     * for the purposes of testing error handling.
     */
    public CoreClusterMember coreTx( String databaseName, Role role, BiConsumer<GraphDatabaseFacade,Transaction> op, int timeout, TimeUnit timeUnit,
                                     ThrowingFunction<GraphDatabaseFacade, LoginContext, InvalidAuthTokenException> securityContextProvider )
            throws Exception
    {
        ThrowingSupplier<CoreClusterMember,Exception> supplier = () ->
        {
            CoreClusterMember member = awaitCoreMemberWithRole( databaseName, role, timeout, timeUnit );
            GraphDatabaseFacade db = (GraphDatabaseFacade) member.managementService().database( databaseName );
            if ( db == null )
            {
                throw new DatabaseShutdownException();
            }

            try ( Transaction tx = db.beginTransaction( KernelTransaction.Type.EXPLICIT, securityContextProvider.apply( db )) )
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
        return Exceptions.contains( e, new TransientFailurePredicate() );
    }

    private List<SocketAddress> extractInitialHosts( Map<Integer,CoreClusterMember> coreMembers )
    {
        return coreMembers.values().stream()
                .map( CoreClusterMember::discoveryAddress )
                .collect( toList() );
    }

    private void createCoreMembers( final int noOfCoreMembers,
            List<SocketAddress> initialHosts, Map<String,String> extraParams,
            Map<String,IntFunction<String>> instanceExtraParams, String recordFormat )
    {
        for ( int i = 0; i < initialHosts.size(); i++ )
        {
            int discoveryListenPort = initialHosts.get( i ).getPort();
            CoreClusterMember coreClusterMember = createCoreClusterMember(
                    i,
                    discoveryListenPort,
                    noOfCoreMembers,
                    initialHosts,
                    recordFormat,
                    extraParams,
                    instanceExtraParams
            );
            coreMembers.put( i, coreClusterMember );
        }
        highestCoreIndex = noOfCoreMembers - 1;
    }

    private CoreClusterMember createCoreClusterMember( int index,
            int discoveryPort,
            int clusterSize,
            List<SocketAddress> initialHosts,
            String recordFormat,
            Map<String,String> extraParams,
            Map<String,IntFunction<String>> instanceExtraParams )
    {
        var txPort = PortAuthority.allocatePort();
        var raftPort = PortAuthority.allocatePort();
        var boltPort = PortAuthority.allocatePort();
        var intraClusterBoltPort = PortAuthority.allocatePort();
        var httpPort = PortAuthority.allocatePort();
        var backupPort = PortAuthority.allocatePort();
        var listenHost = getListenHost( index, extraParams, instanceExtraParams );
        var advertisedHost = getAdvertisedHost( index, extraParams, instanceExtraParams );

        return new CoreClusterMember(
                index,
                discoveryPort,
                txPort,
                raftPort,
                boltPort,
                intraClusterBoltPort,
                httpPort,
                backupPort,
                clusterSize,
                initialHosts,
                discoveryServiceFactory,
                recordFormat,
                parentDir,
                extraParams,
                instanceExtraParams,
                listenHost,
                advertisedHost
        );
    }

    private ReadReplica createReadReplica( int index,
            List<SocketAddress> initialHosts,
            Map<String,String> extraParams,
            Map<String,IntFunction<String>> instanceExtraParams,
            String recordFormat,
            Monitors monitors )
    {
        var boltPort = PortAuthority.allocatePort();
        var intraClusterBoltPort = PortAuthority.allocatePort();
        var httpPort = PortAuthority.allocatePort();
        var txPort = PortAuthority.allocatePort();
        var backupPort = PortAuthority.allocatePort();
        var discoveryPort = PortAuthority.allocatePort();
        var listenHost = getListenHost( index, extraParams, instanceExtraParams );
        var advertisedHost = getAdvertisedHost( index, extraParams, instanceExtraParams );

        return new ReadReplica(
                parentDir,
                index,
                boltPort,
                intraClusterBoltPort,
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
                advertisedHost,
                listenHost,
                ( Config config, GraphDatabaseDependencies dependencies, DiscoveryServiceFactory discoveryServiceFactory ) ->
                        new TestReadReplicaGraphDatabase( config, dependencies, discoveryServiceFactory, ReadReplicaEditionModule::new )
        );
    }

    private String getListenHost( int index, Map<String,String> extraParams, Map<String,IntFunction<String>> instanceExtraParams )
    {
        return getHost( index, extraParams, instanceExtraParams, default_listen_address, defaultListenHost );
    }

    private String getAdvertisedHost( int index, Map<String,String> extraParams, Map<String,IntFunction<String>> instanceExtraParams )
    {
        return getHost( index, extraParams, instanceExtraParams, default_advertised_address, defaultAdvertisedHost );
    }

    private String getHost( int index, Map<String,String> extraParams, Map<String,IntFunction<String>> instanceExtraParams,
            Setting<SocketAddress> hostSetting, String defaultHost )
    {
        IntFunction<String> def = ignored -> extraParams.getOrDefault( hostSetting.name(), defaultHost );
        return instanceExtraParams
                .getOrDefault( hostSetting.name(), def )
                .apply( index );
    }

    public void startCoreMembers() throws InterruptedException, ExecutionException
    {
        startMembers( coreMembers() );
    }

    public void startReadReplicas() throws InterruptedException, ExecutionException
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
        highestReplicaIndex = noOfReadReplicas - 1;
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

    @Override
    public Set<ServerAddress> resolve( ServerAddress ignore )
    {
        var serverAddresses = coreMembers()
                .stream()
                .map( c -> URI.create( c.routingURI() ) )
                .map( uri -> ServerAddress.of( uri.getHost(), uri.getPort() ) )
                .collect( toList() );
        shuffle( serverAddresses );
        return new LinkedHashSet<>( serverAddresses );
    }
}
