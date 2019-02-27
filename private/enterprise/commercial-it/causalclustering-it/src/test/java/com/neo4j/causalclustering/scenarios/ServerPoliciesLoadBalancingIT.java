/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.DefaultCluster;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.CoreGraphDatabase;
import com.neo4j.causalclustering.discovery.HazelcastDiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.IpFamily;
import com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.Policies;
import com.neo4j.kernel.enterprise.api.security.CommercialLoginContext;
import org.eclipse.collections.impl.factory.Sets;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.procedure.builtin.routing.ParameterNames;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.procedure.builtin.routing.RoutingResultFormat;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.virtual.MapValueBuilder;

import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

@ExtendWith( {TestDirectoryExtension.class, DefaultFileSystemExtension.class} )
class ServerPoliciesLoadBalancingIT
{
    @Inject
    private TestDirectory testDir;
    @Inject
    private FileSystemAbstraction fs;

    private Cluster<?> cluster;

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
            CoreGraphDatabase db = core.database();

            assertThat( getServers( db, policyContext( "default" ) ), new SpecificReplicasMatcher( 0, 1 ) );
            assertThat( getServers( db, policyContext( "policy_one_two" ) ), new SpecificReplicasMatcher( 1, 2 ) );
            assertThat( getServers( db, policyContext( "policy_zero_two" ) ), new SpecificReplicasMatcher( 0, 2 ) );
            assertThat( getServers( db, policyContext( "policy_all_replicas" ) ), new SpecificReplicasMatcher( 0, 1, 2 ) );
        }
    }

    private Cluster<?> startCluster( int cores, int readReplicas, Map<String,String> sharedCoreParams, Map<String,IntFunction<String>> instanceCoreParams,
            Map<String,IntFunction<String>> instanceReplicaParams ) throws Exception
    {
        Cluster<?> cluster = new DefaultCluster( testDir.directory( "cluster" ), cores, readReplicas,
                new HazelcastDiscoveryServiceFactory(), sharedCoreParams, instanceCoreParams,
                emptyMap(), instanceReplicaParams, Standard.LATEST_NAME, IpFamily.IPV4, false );

        cluster.start();

        return cluster;
    }

    private Map<String,String> policyContext( String policyName )
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
            if ( core.database() == null )
            {
                // this core is shutdown
                continue;
            }

            assertEventually( matcher, () -> getServers( core.database(), context ) );
        }
    }

    private RoutingResult getServers( CoreGraphDatabase db, Map<String,String> context )
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
        }
        return lbResult;
    }

    private static <T, E extends Exception> void assertEventually( Matcher<? super T> matcher,
            ThrowingSupplier<T,E> actual ) throws InterruptedException, E
    {
        org.neo4j.test.assertion.Assert.assertEventually( "", actual, matcher, 120, SECONDS );
    }

    class CountsMatcher extends BaseMatcher<RoutingResult>
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
        public boolean matches( Object item )
        {
            RoutingResult result = (RoutingResult) item;

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
}
