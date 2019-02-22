/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.extension.Inject;

import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

@ClusterExtension
class CausalClusteringProceduresIT
{
    private static final String[] PROCEDURES_WITHOUT_PARAMS = {
            "dbms.cluster.role",
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

    private static Cluster<?> cluster;

    @BeforeAll
    void setup() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig().withNumberOfCoreMembers( 2 ).withNumberOfReadReplicas( 1 ) );
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

    private static void testProcedureExistence( String[] procedures, Collection<? extends ClusterMember<?>> members, boolean withContextParameter )
    {
        for ( String procedure : procedures )
        {
            for ( ClusterMember<?> member : members )
            {
                GraphDatabaseService db = member.database();
                try ( Transaction tx = db.beginTx();
                      Result result = invokeProcedure( db, procedure, withContextParameter ) )
                {
                    List<Map<String,Object>> records = Iterators.asList( result );
                    assertThat( records, hasSize( greaterThanOrEqualTo( 1 ) ) );
                    tx.success();
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
}
