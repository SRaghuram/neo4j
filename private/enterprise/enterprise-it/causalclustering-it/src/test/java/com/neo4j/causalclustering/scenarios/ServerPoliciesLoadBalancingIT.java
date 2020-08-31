/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.LoadBalancingServerPoliciesGroup;
import com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.Policies;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.assertj.core.api.HamcrestCondition;
import org.eclipse.collections.impl.factory.Sets;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.function.Predicates;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.procedure.builtin.routing.ParameterNames;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.procedure.builtin.routing.RoutingResultFormat;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.values.virtual.MapValueBuilder;

import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;

@ExtendWith( SuppressOutputExtension.class )
@ClusterExtension
@TestInstance( PER_METHOD )
@ResourceLock( Resources.SYSTEM_OUT )
class ServerPoliciesLoadBalancingIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @Test
    void defaultBehaviourWithoutAllowReadsOnFollowers() throws Exception
    {
        var coreParams = Map.of( CausalClusteringSettings.cluster_allow_reads_on_followers.name(), FALSE );

        cluster = startCluster( 3, 3, coreParams, emptyMap(), emptyMap() );

        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 3, 1, 0, 3 ) );
    }

    @Test
    void defaultBehaviourWithAllowReadsOnFollowers() throws Exception
    {
        var coreParams = Map.of( CausalClusteringSettings.cluster_allow_reads_on_followers.name(), TRUE );

        cluster = startCluster( 3, 3, coreParams, emptyMap(), emptyMap() );

        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 3, 1, 2, 3 ) );
    }

    @Test
    void shouldFallOverBetweenRules() throws Exception
    {
        Map<String,IntFunction<String>> instanceCoreParams = new HashMap<>();
        instanceCoreParams.put( CausalClusteringSettings.server_groups.name(), id -> "core" + id + ",core" );
        Map<String,IntFunction<String>> instanceReplicaParams = new HashMap<>();
        instanceReplicaParams.put( CausalClusteringSettings.server_groups.name(), id -> "replica" + id + ",replica" );

        String defaultPolicy = "groups(core) -> min(3); groups(replica1,replica2) -> min(2);";

        Map<String,String> coreParams = stringMap(
                CausalClusteringSettings.cluster_allow_reads_on_followers.name(), TRUE,
                LoadBalancingServerPoliciesGroup.group( "default" ).value.name(), defaultPolicy,
                CausalClusteringSettings.multi_dc_license.name(), TRUE);

        cluster = startCluster( 5, 5, coreParams, instanceCoreParams, instanceReplicaParams );

        // should use the first rule: only cores for reading
        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 5, 1, 4, 0 ) );

        cluster.getCoreMemberByIndex( 3 ).shutdown();
        // one core reader is gone, but we are still fulfilling min(3)
        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 4, 1, 3, 0 ) );

        cluster.getCoreMemberByIndex( 0 ).shutdown();
        // should now fall over to the second rule: use replica1 and replica2
        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 3, 1, 0, 2 ) );

        cluster.getReadReplicaByIndex( 0 ).shutdown();
        // this does not affect replica1 and replica2
        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 3, 1, 0, 2 ) );

        cluster.getReadReplicaByIndex( 1 ).shutdown();
        // should now fall over to use the last rule: all
        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 3, 1, 2, 3 ) );

        cluster.addCoreMemberWithIndex( 3 ).start();
        // should now go back to first rule
        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 4, 1, 3, 0 ) );
    }

    @Test
    void shouldSupportSeveralPolicies() throws Exception
    {
        Map<String,IntFunction<String>> instanceCoreParams = new HashMap<>();
        instanceCoreParams.put( CausalClusteringSettings.server_groups.name(), id -> "core" + id + ",core" );
        Map<String,IntFunction<String>> instanceReplicaParams = new HashMap<>();
        instanceReplicaParams.put( CausalClusteringSettings.server_groups.name(), id -> "replica" + id + ",replica" );

        String defaultPolicySpec = "groups(replica0,replica1)";
        String policyOneTwoSpec = "groups(replica1,replica2)";
        String policyZeroTwoSpec = "groups(replica0,replica2)";
        String policyAllReplicasSpec = "groups(replica); halt()";
        String allPolicySpec = "all()";

        Map<String,String> coreParams = stringMap(
                CausalClusteringSettings.cluster_allow_reads_on_followers.name(), TRUE,
                LoadBalancingServerPoliciesGroup.group( "all" ).value.name(), allPolicySpec,
                LoadBalancingServerPoliciesGroup.group( "default" ).value.name(), defaultPolicySpec,
                LoadBalancingServerPoliciesGroup.group( "policy_one_two" ).value.name(), policyOneTwoSpec,
                LoadBalancingServerPoliciesGroup.group( "policy_zero_two" ).value.name(), policyZeroTwoSpec,
                LoadBalancingServerPoliciesGroup.group( "policy_all_replicas" ).value.name(), policyAllReplicasSpec,
                CausalClusteringSettings.multi_dc_license.name(), TRUE
        );

        cluster = startCluster( 3, 3, coreParams, instanceCoreParams, instanceReplicaParams );

        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 3, 1, 2, 3 ), policyContext( "all" ) );
        // all cores have observed the full topology, now specific policies should all return the same result

        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            GraphDatabaseFacade db = core.defaultDatabase();

            assertThat( getRoutingTable( db, policyContext( "default" ) ), new SpecificReplicasMatcher( 0, 1 ) );
            assertThat( getRoutingTable( db, policyContext( "policy_one_two" ) ), new SpecificReplicasMatcher( 1, 2 ) );
            assertThat( getRoutingTable( db, policyContext( "policy_zero_two" ) ), new SpecificReplicasMatcher( 0, 2 ) );
            assertThat( getRoutingTable( db, policyContext( "policy_all_replicas" ) ), new SpecificReplicasMatcher( 0, 1, 2 ) );
        }
    }

    @Test
    void shouldPartiallyOrderRoutersByPolicy() throws Exception
    {
        Map<String,IntFunction<String>> instanceCoreParams = new HashMap<>();
        IntFunction<String> oddEven = i -> i % 2 == 0 ? "Even" : "Odd";
        instanceCoreParams.put( CausalClusteringSettings.server_groups.name(), id -> "core,core" + oddEven.apply( id ) );

        String evensPolicy = "groups(coreEven)";
        String evensHaltPolicy = "groups(coreEven);halt()";
        String oddsPolicy = "groups(coreOdd)";
        String oddsMinPolicy = "groups(coreOdd) -> min(3);groups(coreEven)";
        String allPolicy = "all()";

        Map<String,String> coreParams = stringMap(
                CausalClusteringSettings.cluster_allow_reads_on_followers.name(), TRUE,
                LoadBalancingServerPoliciesGroup.group( "evens" ).value.name(), evensPolicy,
                LoadBalancingServerPoliciesGroup.group( "evensHalt" ).value.name(), evensHaltPolicy,
                LoadBalancingServerPoliciesGroup.group( "odds" ).value.name(), oddsPolicy,
                LoadBalancingServerPoliciesGroup.group( "oddsMin" ).value.name(), oddsMinPolicy,
                LoadBalancingServerPoliciesGroup.group( "all" ).value.name(), allPolicy,
                CausalClusteringSettings.multi_dc_license.name(), TRUE );

        cluster = startCluster( 5, 0, coreParams, instanceCoreParams, emptyMap() );

        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 5, 1, 4, 0 ), policyContext( "all" ) );
        // all cores have observed the full topology, now specific policies should all return the same result

        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            GraphDatabaseFacade db = core.defaultDatabase();

            assertThat( getRoutingTable( db, policyContext( "evens" ) ),
                    new RouterPartialOrderMatcher( false, asSet( 2 ), asSet( 1, 3 ) ) );
            assertThat( getRoutingTable( db, policyContext( "odds" ) ),
                    new RouterPartialOrderMatcher( false, asSet( 1, 3 ), asSet( 0, 2 ) ) );
            assertThat( getRoutingTable( db, policyContext( "evens" ) ),
                    new RouterPartialOrderMatcher( true, asSet( 0, 2, 4 ), asSet( 1, 3 ) ) );
            assertThat( getRoutingTable( db, policyContext( "evensHalt" ) ),
                    new RouterPartialOrderMatcher( true, asSet( 0, 2, 4 ), asSet( 1, 3 ) ) );
            assertThat( getRoutingTable( db, policyContext( "oddsMin" ) ),
                    new RouterPartialOrderMatcher( true, asSet( 0, 2, 4 ), asSet( 1, 3 ) ) );
        }
    }

    private Cluster startCluster( int cores, int readReplicas, Map<String,String> sharedCoreParams, Map<String,IntFunction<String>> instanceCoreParams,
            Map<String,IntFunction<String>> instanceReplicaParams ) throws Exception
    {
        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( cores )
                .withNumberOfReadReplicas( readReplicas )
                .withSharedCoreParams( sharedCoreParams )
                .withInstanceCoreParams( instanceCoreParams )
                .withInstanceReadReplicaParams( instanceReplicaParams );

        var cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        return cluster;
    }

    private static Map<String,String> policyContext( String policyName )
    {
        return stringMap( Policies.POLICY_KEY, policyName );
    }

    private void assertGetServersEventuallyMatchesOnAllCores( Matcher<RoutingResult> matcher ) throws Exception
    {
        assertGetServersEventuallyMatchesOnAllCores( matcher, emptyMap() );
    }

    private void assertGetServersEventuallyMatchesOnAllCores( Matcher<RoutingResult> matcher, Map<String,String> context )
    {
        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            if ( core.isShutdown() )
            {
                continue;
            }

            assertEventually( matcher, () -> getRoutingTable( core.defaultDatabase(), context ) );
        }
    }

    private static RoutingResult getRoutingTable( GraphDatabaseFacade db, Map<String,String> context )
    {
        try ( var tx = db.beginTransaction( KernelTransaction.Type.EXPLICIT, EnterpriseLoginContext.AUTH_DISABLED ) )
        {
            var parameters = MapUtil.map( ParameterNames.CONTEXT.parameterName(), context );
            try ( var result = tx.execute( "CALL dbms.routing.getRoutingTable", parameters ) )
            {
                var record = Iterators.single( result );
                var mapValueBuilder = new MapValueBuilder();
                record.forEach( ( k, v ) -> mapValueBuilder.add( k, ValueUtils.of( v ) ) );
                return RoutingResultFormat.parse( mapValueBuilder.build() );
            }
        }
        catch ( Exception e )
        {
            // ignore errors because this cluster member might have not yet started the default database
            // stacktrace will only be printed when a test fails
            e.printStackTrace();
            return null;
        }
    }

    private static <T> void assertEventually( Matcher<? super T> matcher, Callable<T> actual )
    {
        org.neo4j.test.assertion.Assert.assertEventually( "", actual, new HamcrestCondition<>( matcher ), 120, SECONDS );
    }

    class CountsMatcher extends TypeSafeMatcher<RoutingResult>
    {
        private final int nRouters;
        private final int nWriters;
        private final int nCoreReaders;
        private final int nReplicaReaders;

        CountsMatcher( int nRouters, int nWriters, int nCoreReaders, int nReplicaReaders )
        {
            this.nRouters = nRouters;
            this.nWriters = nWriters;
            this.nCoreReaders = nCoreReaders;
            this.nReplicaReaders = nReplicaReaders;
        }

        @Override
        public boolean matchesSafely( RoutingResult result )
        {
            if ( result.routeEndpoints().size() != nRouters ||
                 result.writeEndpoints().size() != nWriters )
            {
                return false;
            }

            Set<SocketAddress> allCoreBolts = cluster.coreMembers().stream()
                    .map( c -> c.clientConnectorAddresses().clientBoltAddress() )
                    .collect( Collectors.toSet() );

            Set<SocketAddress> returnedCoreReaders = result.readEndpoints().stream()
                    .filter( allCoreBolts::contains )
                    .collect( Collectors.toSet() );

            if ( returnedCoreReaders.size() != nCoreReaders )
            {
                return false;
            }

            Set<SocketAddress> allReplicaBolts = cluster.readReplicas().stream()
                    .map( c -> c.clientConnectorAddresses().clientBoltAddress() )
                    .collect( Collectors.toSet() );

            Set<SocketAddress> returnedReplicaReaders = result.readEndpoints().stream()
                    .filter( allReplicaBolts::contains )
                    .collect( Collectors.toSet() );

            if ( returnedReplicaReaders.size() != nReplicaReaders )
            {
                return false;
            }

            HashSet<SocketAddress> overlap = new HashSet<>( returnedCoreReaders );
            overlap.retainAll( returnedReplicaReaders );

            if ( !overlap.isEmpty() )
            {
                return false;
            }

            Set<SocketAddress> returnedWriters = new HashSet<>( result.writeEndpoints() );

            if ( !allCoreBolts.containsAll( returnedWriters ) )
            {
                return false;
            }

            Set<SocketAddress> allBolts = Sets.union( allCoreBolts, allReplicaBolts );
            Set<SocketAddress> returnedRouters = new HashSet<>( result.routeEndpoints() );

            //noinspection RedundantIfStatement
            if ( !allBolts.containsAll( returnedRouters ) )
            {
                return false;
            }

            return true;
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "nRouters=" + nRouters );
            description.appendText( ", nWriters=" + nWriters );
            description.appendText( ", nCoreReaders=" + nCoreReaders );
            description.appendText( ", nReplicaReaders=" + nReplicaReaders );
        }
    }

    class SpecificReplicasMatcher extends BaseMatcher<RoutingResult>
    {
        private final Set<Integer> replicaIds;

        SpecificReplicasMatcher( Integer... replicaIds )
        {
            this.replicaIds = Arrays.stream( replicaIds ).collect( Collectors.toSet() );
        }

        @Override
        public boolean matches( Object item )
        {
            RoutingResult result = (RoutingResult) item;

            Set<SocketAddress> returnedReaders = new HashSet<>( result.readEndpoints() );

            Set<SocketAddress> expectedBolts = cluster.readReplicas().stream()
                    .filter( r -> replicaIds.contains( r.index() ) )
                    .map( r -> r.clientConnectorAddresses().clientBoltAddress() )
                    .collect( Collectors.toSet() );

            return expectedBolts.equals( returnedReaders );
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "replicaIds=" + replicaIds );
        }
    }

    class RouterPartialOrderMatcher extends BaseMatcher<RoutingResult>
    {
        private final boolean exactMatch;
        private final List<Set<Integer>> subsets;

        @SafeVarargs
        RouterPartialOrderMatcher( boolean exactMatch, Set<Integer>... subsets )
        {
            this.exactMatch = exactMatch;
            this.subsets = Arrays.asList( subsets );
        }

        @Override
        public boolean matches( Object item )
        {
            RoutingResult result = (RoutingResult) item;

            List<SocketAddress> returnedRouters = new ArrayList<>( result.routeEndpoints() );

            Map<Integer,SocketAddress> allBoltsById = cluster.coreMembers().stream()
                    .collect( Collectors.toMap( CoreClusterMember::index, c -> c.clientConnectorAddresses().clientBoltAddress() ) );

            Function<Set<Integer>,Set<SocketAddress>> lookupBoltSubsets =
                    s -> s.stream().map( i -> getAddressOrThrow( allBoltsById, i ) ).collect( Collectors.toSet() );

            List<Set<SocketAddress>> expectedBoltSubsets = subsets.stream()
                    .map( lookupBoltSubsets )
                    .collect( Collectors.toList() );

            Set<SocketAddress> allExpectedBolts = subsets.stream()
                    .map( lookupBoltSubsets )
                    .flatMap( Set::stream )
                    .collect( Collectors.toSet() );

            Predicate<SocketAddress> filterReturnedRouters = exactMatch ? Predicates.alwaysTrue() : allExpectedBolts::contains;

            List<Integer> orders = returnedRouters.stream()
                    .filter( filterReturnedRouters )
                    .map( address -> findPartialOrderForAddress( expectedBoltSubsets, address ) )
                    .collect( Collectors.toList() );

            List<Integer> sortedOrders = new ArrayList<>( orders );
            Collections.sort( sortedOrders );

            return orders.equals( sortedOrders );
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "expectedRouterOrder=" + subsets );
        }

        private int findPartialOrderForAddress( List<Set<SocketAddress>> addressSubsets, SocketAddress address )
        {
            for ( int i = 0; i < addressSubsets.size(); i++ )
            {
                if ( addressSubsets.get( i ).contains( address ) )
                {
                    return i;
                }
            }

            throw new IllegalStateException( format( "An unexpected member has been returned! Expected:%s, Offender:%s",
                    addressSubsets, address ) );
        }

        private SocketAddress getAddressOrThrow( Map<Integer,SocketAddress> addressMap, int idx )
        {
            SocketAddress address = addressMap.get( idx );
            if ( address == null )
            {
                throw new IllegalArgumentException( format( "You have expected member ids which do not exist! Expected:%s, Actual:%s",
                        subsets, addressMap.keySet() ) );
            }
            return address;
        }
    }
}
