/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.CoreGraphDatabase;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.discovery.DiscoveryServiceType;
import com.neo4j.causalclustering.routing.Endpoint;
import com.neo4j.causalclustering.routing.multi_cluster.MultiClusterRoutingResult;
import com.neo4j.causalclustering.routing.multi_cluster.procedure.MultiClusterRoutingResultFormat;
import com.neo4j.causalclustering.routing.multi_cluster.procedure.ProcedureNames;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.graphdb.Result;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.routing.multi_cluster.procedure.ParameterNames.DATABASE;
import static com.neo4j.causalclustering.routing.multi_cluster.procedure.ProcedureNames.GET_ROUTERS_FOR_ALL_DATABASES;
import static com.neo4j.causalclustering.routing.multi_cluster.procedure.ProcedureNames.GET_ROUTERS_FOR_DATABASE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
@ExtendWith( {DefaultFileSystemExtension.class} )
public abstract class BaseMultiClusterRoutingIT
{

    protected static Set<String> DB_NAMES_1 = Stream.of( "foo", "bar" ).collect( Collectors.toSet() );
    static Set<String> DB_NAMES_2 = Collections.singleton( "default" );
    static Set<String> DB_NAMES_3 = Stream.of( "foo", "bar", "baz" ).collect( Collectors.toSet() );

    private final Set<String> dbNames;
    private final ClusterConfig clusterConfig;
    private final int numCores;

    private Cluster<?> cluster;

    @Inject
    private DefaultFileSystemAbstraction fs;

    @Inject
    private ClusterFactory clusterFactory;

    protected BaseMultiClusterRoutingIT( int numCores, int numReplicas, Set<String> dbNames, DiscoveryServiceType discoveryType )
    {
        this.dbNames = dbNames;
        this.clusterConfig = ClusterConfig
                .clusterConfig()
                .withNumberOfCoreMembers( numCores )
                .withNumberOfReadReplicas( numReplicas )
                .withDatabaseNames( dbNames )
                .withDiscoveryServiceType( discoveryType );
        this.numCores = numCores;
    }

    @BeforeAll
    void setup() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void superCallShouldReturnAllRouters()
    {
        List<CoreGraphDatabase> dbs = dbNames.stream()
                .map( n -> cluster.getMemberWithAnyRole( n, Role.FOLLOWER, Role.LEADER ).database() ).collect( Collectors.toList() );

        Stream<Optional<MultiClusterRoutingResult>> optResults = dbs.stream()
                .map( db -> callProcedure( db, GET_ROUTERS_FOR_ALL_DATABASES, Collections.emptyMap() ) );

        List<MultiClusterRoutingResult> results = optResults.filter( Optional::isPresent ).map( Optional::get ).collect( Collectors.toList() );
        assertEquals( dbNames.size(), results.size(), "There should be a result for each database against which the procedure is executed." );

        boolean consistentResults = results.stream().distinct().count() == 1;
        assertThat( "The results should be the same, regardless of which database the procedure is executed against.", consistentResults );

        Function<Map<String,List<Endpoint>>, Integer> countHosts = m -> m.values().stream().mapToInt( List::size ).sum();
        int resultsAllCores = results.stream().findFirst().map(r -> countHosts.apply( r.routers() ) ).orElse( 0 );
        assertEquals( numCores, resultsAllCores, "The results of the procedure should return all core hosts in the topology." );
    }

    @Test
    void subCallShouldReturnLocalRouters()
    {
        String dbName = getFirstDbName( dbNames );
        Stream<CoreGraphDatabase> members = dbNames.stream().map( n -> cluster.getMemberWithAnyRole( n, Role.FOLLOWER, Role.LEADER ).database() );

        Map<String,Object> params = new HashMap<>();
        params.put( DATABASE.parameterName(), dbName );
        Stream<Optional<MultiClusterRoutingResult>> optResults = members.map( db -> callProcedure( db, GET_ROUTERS_FOR_DATABASE, params ) );
        List<MultiClusterRoutingResult> results = optResults.filter( Optional::isPresent ).map( Optional::get ).collect( Collectors.toList() );

        boolean consistentResults = results.stream().distinct().count() == 1;
        assertThat( "The results should be the same, regardless of which database the procedure is executed against.", consistentResults );

        Optional<MultiClusterRoutingResult> firstResult = results.stream().findFirst();

        int numRouterSets = firstResult.map( r -> r.routers().size() ).orElse( 0 );
        assertEquals( 1, numRouterSets, "There should only be routers returned for a single database." );

        boolean correctResultDbName = firstResult.map( r -> r.routers().containsKey( dbName ) ).orElse( false );
        assertThat( "The results should contain routers for the database passed to the procedure.", correctResultDbName );
    }

    @Test
    void procedureCallsShouldReflectMembershipChanges() throws Exception
    {
        String dbName = getFirstDbName( dbNames );
        CoreClusterMember follower = cluster.getMemberWithAnyRole( dbName, Role.FOLLOWER );
        int followerId = follower.serverId();

        cluster.removeCoreMemberWithServerId( followerId );

        CoreGraphDatabase db = cluster.getMemberWithAnyRole( dbName, Role.FOLLOWER, Role.LEADER ).database();

        Function<CoreGraphDatabase, Set<Endpoint>> getResult = database ->
        {
            Optional<MultiClusterRoutingResult> optResult = callProcedure( database, GET_ROUTERS_FOR_ALL_DATABASES, Collections.emptyMap() );

            return optResult.map( r ->
                    r.routers().values().stream()
                            .flatMap( List::stream )
                            .collect( Collectors.toSet() )
            ).orElse( Collections.emptySet() );
        };

        assertEventually( "The procedure should return one fewer routers when a core member has been removed.",
                () -> getResult.apply( db ).size(), is(numCores - 1 ), 15, TimeUnit.SECONDS );

        BiPredicate<Set<Endpoint>, CoreClusterMember> containsFollower = ( rs, f ) ->
                rs.stream().anyMatch( r -> r.address().toString().equals( f.boltAdvertisedAddress() ) );

        assertEventually( "The procedure should not return a host as a router after it has been removed from the cluster",
                () -> containsFollower.test( getResult.apply( db ), follower ), is( false ), 15, TimeUnit.SECONDS );

        CoreClusterMember newFollower = cluster.addCoreMemberWithId( followerId );
        newFollower.start();

        assertEventually( "The procedure should return one more router when a core member has been added.",
                () -> getResult.apply( db ).size(), is( numCores ), 15, TimeUnit.SECONDS );
        assertEventually( "The procedure should return a core member as a router after it has been added to the cluster",
                () -> containsFollower.test( getResult.apply( db ), newFollower ), is( true ), 15, TimeUnit.SECONDS );

    }

    private static String getFirstDbName( Set<String> dbNames )
    {
        return dbNames.stream()
                .findFirst()
                .orElseThrow( () -> new IllegalArgumentException( "The dbNames parameter must not be empty." ) );
    }

    private static Optional<MultiClusterRoutingResult> callProcedure( CoreGraphDatabase db, ProcedureNames procedure, Map<String,Object> params )
    {

        Optional<MultiClusterRoutingResult> routingResult = Optional.empty();
        try (
                InternalTransaction tx = db.beginTransaction( KernelTransaction.Type.explicit, EnterpriseLoginContext.AUTH_DISABLED );
                Result result = db.execute( tx, "CALL " + procedure.callName(), ValueUtils.asMapValue( params )) )
        {
            if ( result.hasNext() )
            {
                routingResult = Optional.of( MultiClusterRoutingResultFormat.parse( result.next() ) );
            }
        }
        return routingResult;
    }

}
