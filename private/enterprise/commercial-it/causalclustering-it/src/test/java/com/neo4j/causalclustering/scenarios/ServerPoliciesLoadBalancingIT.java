/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.discovery.IpFamily;
import com.neo4j.causalclustering.discovery.akka.AkkaDiscoveryServiceFactory;
import com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.Policies;
import com.neo4j.kernel.enterprise.api.security.CommercialLoginContext;
import org.eclipse.collections.impl.factory.Sets;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.neo4j.function.Predicates;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.helpers.AdvertisedSocketAddress;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.procedure.builtin.routing.ParameterNames;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.procedure.builtin.routing.RoutingResultFormat;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.virtual.MapValueBuilder;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseNotFound;

@ExtendWith( TestDirectoryExtension.class )
class ServerPoliciesLoadBalancingIT
{
    @Inject
    private TestDirectory testDir;

    private Cluster cluster;

    @AfterEach
    void AfterEach()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    void defaultBehaviour() throws Exception
    {
        cluster = startCluster( 3, 3, emptyMap(), emptyMap(), emptyMap() );

        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 3, 1, 2, 3 ) );
    }

    @Test
    void defaultBehaviourWithAllowReadsOnFollowers() throws Exception
    {
        cluster = startCluster( 3, 3, emptyMap(), emptyMap(), emptyMap() );

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
                CausalClusteringSettings.cluster_allow_reads_on_followers.name(), "true",
                CausalClusteringSettings.load_balancing_config.name() + ".server_policies.default", defaultPolicy,
                CausalClusteringSettings.multi_dc_license.name(), "true");

        cluster = startCluster( 5, 5, coreParams, instanceCoreParams, instanceReplicaParams );

        // should use the first rule: only cores for reading
        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 5, 1, 4, 0 ) );

        cluster.getCoreMemberById( 3 ).shutdown();
        // one core reader is gone, but we are still fulfilling min(3)
        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 4, 1, 3, 0 ) );

        cluster.getCoreMemberById( 0 ).shutdown();
        // should now fall over to the second rule: use replica1 and replica2
        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 3, 1, 0, 2 ) );

        cluster.getReadReplicaById( 0 ).shutdown();
        // this does not affect replica1 and replica2
        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 3, 1, 0, 2 ) );

        cluster.getReadReplicaById( 1 ).shutdown();
        // should now fall over to use the last rule: all
        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 3, 1, 2, 3 ) );

        cluster.addCoreMemberWithId( 3 ).start();
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
                CausalClusteringSettings.cluster_allow_reads_on_followers.name(), "true",
                CausalClusteringSettings.load_balancing_config.name() + ".server_policies.all", allPolicySpec,
                CausalClusteringSettings.load_balancing_config.name() + ".server_policies.default", defaultPolicySpec,
                CausalClusteringSettings.load_balancing_config.name() + ".server_policies.policy_one_two", policyOneTwoSpec,
                CausalClusteringSettings.load_balancing_config.name() + ".server_policies.policy_zero_two", policyZeroTwoSpec,
                CausalClusteringSettings.load_balancing_config.name() + ".server_policies.policy_all_replicas", policyAllReplicasSpec,
                CausalClusteringSettings.multi_dc_license.name(), "true"
        );

        cluster = startCluster( 3, 3, coreParams, instanceCoreParams, instanceReplicaParams );

        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 3, 1, 2, 3 ), policyContext( "all" ) );
        // all cores have observed the full topology, now specific policies should all return the same result

        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            GraphDatabaseFacade db = core.defaultDatabase();

            assertThat( getServers( db, policyContext( "default" ) ), new SpecificReplicasMatcher( 0, 1 ) );
            assertThat( getServers( db, policyContext( "policy_one_two" ) ), new SpecificReplicasMatcher( 1, 2 ) );
            assertThat( getServers( db, policyContext( "policy_zero_two" ) ), new SpecificReplicasMatcher( 0, 2 ) );
            assertThat( getServers( db, policyContext( "policy_all_replicas" ) ), new SpecificReplicasMatcher( 0, 1, 2 ) );
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
                CausalClusteringSettings.cluster_allow_reads_on_followers.name(), "true",
                CausalClusteringSettings.load_balancing_config.name() + ".server_policies.evens", evensPolicy,
                CausalClusteringSettings.load_balancing_config.name() + ".server_policies.evensHalt", evensHaltPolicy,
                CausalClusteringSettings.load_balancing_config.name() + ".server_policies.odds", oddsPolicy,
                CausalClusteringSettings.load_balancing_config.name() + ".server_policies.oddsMin", oddsMinPolicy,
                CausalClusteringSettings.load_balancing_config.name() + ".server_policies.all", allPolicy,
                CausalClusteringSettings.multi_dc_license.name(), "true" );

        cluster = startCluster( 5, 0, coreParams, instanceCoreParams, emptyMap() );

        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 5, 1, 4, 0 ), policyContext( "all" ) );
        // all cores have observed the full topology, now specific policies should all return the same result

        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            GraphDatabaseFacade db = core.defaultDatabase();

            assertThat( getServers( db, policyContext( "evens" ) ),
                    new RouterPartialOrderMatcher( false, asSet( 2 ), asSet( 1, 3 ) ) );
            assertThat( getServers( db, policyContext( "odds" ) ),
                    new RouterPartialOrderMatcher( false, asSet( 1, 3 ), asSet( 0, 2 ) ) );
            assertThat( getServers( db, policyContext( "evens" ) ),
                    new RouterPartialOrderMatcher( true, asSet( 0, 2, 4 ), asSet( 1, 3 ) ) );
            assertThat( getServers( db, policyContext( "evensHalt" ) ),
                    new RouterPartialOrderMatcher( true, asSet( 0, 2, 4 ), asSet( 1, 3 ) ) );
            assertThat( getServers( db, policyContext( "oddsMin" ) ),
                    new RouterPartialOrderMatcher( true, asSet( 0, 2, 4 ), asSet( 1, 3 ) ) );
        }
    }

    private Cluster startCluster( int cores, int readReplicas, Map<String,String> sharedCoreParams, Map<String,IntFunction<String>> instanceCoreParams,
            Map<String,IntFunction<String>> instanceReplicaParams ) throws Exception
    {
        Cluster cluster = new Cluster( testDir.directory( "cluster" ), cores, readReplicas,
                new AkkaDiscoveryServiceFactory(), sharedCoreParams, instanceCoreParams,
                emptyMap(), instanceReplicaParams, Standard.LATEST_NAME, IpFamily.IPV4, false );

        cluster.start();

        return cluster;
    }

    private static Map<String,String> policyContext( String policyName )
    {
        return stringMap( Policies.POLICY_KEY, policyName );
    }

    private void assertGetServersEventuallyMatchesOnAllCores( Matcher<RoutingResult> matcher ) throws InterruptedException
    {
        assertGetServersEventuallyMatchesOnAllCores( matcher, emptyMap() );
    }

    private void assertGetServersEventuallyMatchesOnAllCores( Matcher<RoutingResult> matcher,
            Map<String,String> context ) throws InterruptedException
    {
        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            if ( core.isShutdown() )
            {
                continue;
            }

            assertEventually( matcher, () -> getServers( core.defaultDatabase(), context ) );
        }
    }

    private static RoutingResult getServers( GraphDatabaseFacade db, Map<String,String> context )
    {
        RoutingResult lbResult = null;
        try ( InternalTransaction tx = db.beginTransaction( KernelTransaction.Type.explicit, CommercialLoginContext.AUTH_DISABLED ) )
        {
            Map<String,Object> parameters = MapUtil.map( ParameterNames.CONTEXT.parameterName(), context );
            try ( Result result = db.execute( tx, "CALL dbms.routing.getRoutingTable", ValueUtils.asMapValue( parameters ) ) )
            {
                while ( result.hasNext() )
                {
                    Map<String,Object> next = result.next();
                    MapValueBuilder builder = new MapValueBuilder();
                    next.forEach( ( k, v ) -> builder.add( k, ValueUtils.of( v ) ) );

                    lbResult = RoutingResultFormat.parse( builder.build() );
                }
            }
            catch ( QueryExecutionException e )
            {
                // ignore database not found errors because this cluster member might have not yet started the default database
                if ( !DatabaseNotFound.code().serialize().equals( e.getStatusCode() ) )
                {
                    throw e;
                }
            }
        }
        return lbResult;
    }

    private static <T, E extends Exception> void assertEventually( Matcher<? super T> matcher,
            ThrowingSupplier<T,E> actual ) throws InterruptedException, E
    {
        org.neo4j.test.assertion.Assert.assertEventually( "", actual, matcher, 120, SECONDS );
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

            Set<AdvertisedSocketAddress> allCoreBolts = cluster.coreMembers().stream()
                    .map( c -> c.clientConnectorAddresses().boltAddress() )
                    .collect( Collectors.toSet() );

            Set<AdvertisedSocketAddress> returnedCoreReaders = result.readEndpoints().stream()
                    .filter( allCoreBolts::contains )
                    .collect( Collectors.toSet() );

            if ( returnedCoreReaders.size() != nCoreReaders )
            {
                return false;
            }

            Set<AdvertisedSocketAddress> allReplicaBolts = cluster.readReplicas().stream()
                    .map( c -> c.clientConnectorAddresses().boltAddress() )
                    .collect( Collectors.toSet() );

            Set<AdvertisedSocketAddress> returnedReplicaReaders = result.readEndpoints().stream()
                    .filter( allReplicaBolts::contains )
                    .collect( Collectors.toSet() );

            if ( returnedReplicaReaders.size() != nReplicaReaders )
            {
                return false;
            }

            HashSet<AdvertisedSocketAddress> overlap = new HashSet<>( returnedCoreReaders );
            overlap.retainAll( returnedReplicaReaders );

            if ( !overlap.isEmpty() )
            {
                return false;
            }

            Set<AdvertisedSocketAddress> returnedWriters = new HashSet<>( result.writeEndpoints() );

            if ( !allCoreBolts.containsAll( returnedWriters ) )
            {
                return false;
            }

            Set<AdvertisedSocketAddress> allBolts = Sets.union( allCoreBolts, allReplicaBolts );
            Set<AdvertisedSocketAddress> returnedRouters = new HashSet<>( result.routeEndpoints() );

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

            Set<AdvertisedSocketAddress> returnedReaders = new HashSet<>( result.readEndpoints() );

            Set<AdvertisedSocketAddress> expectedBolts = cluster.readReplicas().stream()
                    .filter( r -> replicaIds.contains( r.serverId() ) )
                    .map( r -> r.clientConnectorAddresses().boltAddress() )
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

            List<AdvertisedSocketAddress> returnedRouters = new ArrayList<>( result.routeEndpoints() );

            Map<Integer,AdvertisedSocketAddress> allBoltsById = cluster.coreMembers().stream()
                    .collect( Collectors.toMap( CoreClusterMember::serverId, c -> c.clientConnectorAddresses().boltAddress() ) );

            Function<Set<Integer>,Set<AdvertisedSocketAddress>> lookupBoltSubsets =
                    s -> s.stream().map( i -> getAddressOrThrow( allBoltsById, i ) ).collect( Collectors.toSet() );

            List<Set<AdvertisedSocketAddress>> expectedBoltSubsets = subsets.stream()
                    .map( lookupBoltSubsets )
                    .collect( Collectors.toList() );

            Set<AdvertisedSocketAddress> allExpectedBolts = subsets.stream()
                    .map( lookupBoltSubsets )
                    .flatMap( Set::stream )
                    .collect( Collectors.toSet() );

            Predicate<AdvertisedSocketAddress> filterReturnedRouters = exactMatch ? Predicates.alwaysTrue() : allExpectedBolts::contains;

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

        private int findPartialOrderForAddress( List<Set<AdvertisedSocketAddress>> addressSubsets, AdvertisedSocketAddress address )
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

        private AdvertisedSocketAddress getAddressOrThrow( Map<Integer,AdvertisedSocketAddress> addressMap, int idx )
        {
            AdvertisedSocketAddress address = addressMap.get( idx );
            if ( address == null )
            {
                throw new IllegalArgumentException( format( "You have expected member ids which do not exist! Expected:%s, Actual:%s",
                        subsets, addressMap.keySet() ) );
            }
            return address;
        }
    }
}
