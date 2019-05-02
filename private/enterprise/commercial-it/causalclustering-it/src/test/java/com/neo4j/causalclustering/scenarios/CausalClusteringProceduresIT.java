/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.ClusterOverviewHelper.assertEventualOverview;
import static com.neo4j.causalclustering.common.ClusterOverviewHelper.containsRole;
import static com.neo4j.causalclustering.discovery.DiscoveryServiceType.AKKA;
import static com.neo4j.causalclustering.discovery.RoleInfo.FOLLOWER;
import static com.neo4j.causalclustering.discovery.RoleInfo.LEADER;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
class CausalClusteringProceduresIT
{
    private static final String[] PROCEDURES_WITHOUT_PARAMS = {
            "dbms.cluster.overview",
            "dbms.procedures",
            "dbms.listQueries"
    };

    private static final String[] PROCEDURES_WITH_CONTEXT_PARAM = {
            "dbms.routing.getRoutingTable",
            "dbms.cluster.routing.getRoutingTable",
    };

    @Inject
    private static ClusterFactory clusterFactory;

    private static Cluster cluster;

    @BeforeAll
    void setup() throws Exception
    {
        var clusterConfig = clusterConfig()
                .withDiscoveryServiceType( AKKA )
                .withNumberOfCoreMembers( 2 )
                .withNumberOfReadReplicas( 1 );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void coreProceduresShouldBeAvailable()
    {
        testProcedureExistence( PROCEDURES_WITHOUT_PARAMS, cluster.coreMembers(), false );
    }

    @Test
    void coreProceduresWithContextParamShouldBeAvailable()
    {
        testProcedureExistence( PROCEDURES_WITH_CONTEXT_PARAM, cluster.coreMembers(), true );
    }

    @Test
    void readReplicaProceduresShouldBeAvailable()
    {
        testProcedureExistence( PROCEDURES_WITHOUT_PARAMS, cluster.readReplicas(), false );
    }

    @Test
    void readReplicaProceduresWithContextParamShouldBeAvailable()
    {
        testProcedureExistence( PROCEDURES_WITH_CONTEXT_PARAM, cluster.readReplicas(), true );
    }

    @Test
    void clusterRoleProcedure() throws Exception
    {
        var databaseId = new DatabaseId( DEFAULT_DATABASE_NAME );
        var leader = cluster.awaitLeader();

        for ( var member : cluster.coreMembers() )
        {
            var expectedRole = Objects.equals( member, leader ) ? RoleInfo.LEADER : RoleInfo.FOLLOWER;
            assertEventually( roleReportedByProcedure( member, databaseId ), equalTo( expectedRole ), 2, MINUTES );
        }

        for ( var member : cluster.readReplicas() )
        {
            var expectedRole = RoleInfo.READ_REPLICA;
            assertEventually( roleReportedByProcedure( member, databaseId ), equalTo( expectedRole ), 2, MINUTES );
        }
    }

    @Test
    void clusterRoleProcedureAfterFollowerShutdown() throws Exception
    {
        var databaseId = new DatabaseId( DEFAULT_DATABASE_NAME );
        var leader = cluster.awaitLeader();
        var follower = cluster.getMemberWithAnyRole( databaseId, Role.FOLLOWER );

        assertThat( cluster.coreMembers(), hasSize( 2 ) );

        try
        {
            // shutdown the only follower and wait for the leader to become a follower
            follower.shutdown();
            assertEventually( roleReportedByProcedure( leader, databaseId ), equalTo( RoleInfo.FOLLOWER ), 2, MINUTES );
        }
        finally
        {
            // restart the follower so cluster has the same shape as before this test
            follower.start();

            // await until follower views the correct cluster
            assertEventualOverview( allOf(
                    containsRole( LEADER, databaseId, 1 ),
                    containsRole( FOLLOWER, databaseId, 1 ) ), follower );
        }
    }

    private static void testProcedureExistence( String[] procedures, Collection<? extends ClusterMember<?>> members, boolean withContextParameter )
    {
        for ( var procedure : procedures )
        {
            for ( var member : members )
            {
                try ( var result = invokeProcedure( member.database(), procedure, withContextParameter ) )
                {
                    var records = Iterators.asList( result );
                    assertThat( records, hasSize( greaterThanOrEqualTo( 1 ) ) );
                }
            }
        }
    }

    private static Result invokeProcedure( GraphDatabaseService db, String name, boolean withContextParameter )
    {
        if ( withContextParameter )
        {
            return db.execute( "CALL " + name + "($value)", singletonMap( "value", emptyMap() ) );
        }
        else
        {
            return db.execute( "CALL " + name + "()" );
        }
    }

    private static ThrowingSupplier<RoleInfo,RuntimeException> roleReportedByProcedure( ClusterMember<?> member, DatabaseId databaseId )
    {
        return () ->
        {
            var db = member.database();
            try ( var result = db.execute( "CALL dbms.cluster.role($database)", Map.of( "database", databaseId.name() ) ) )
            {
                return RoleInfo.valueOf( (String) Iterators.single( result ).get( "role" ) );
            }
        };
    }
}
