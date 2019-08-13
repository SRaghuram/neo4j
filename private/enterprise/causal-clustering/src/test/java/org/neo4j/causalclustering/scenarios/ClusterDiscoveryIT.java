/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.Transaction.Type;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.cluster_allow_reads_on_followers;
import static org.neo4j.causalclustering.routing.load_balancing.procedure.ProcedureNames.GET_SERVERS_V1;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;

@RunWith( Parameterized.class )
public class ClusterDiscoveryIT
{
    @Parameterized.Parameter( 0 )
    public String ignored; // <- JUnit is happy only if this is here!
    @Parameterized.Parameter( 1 )
    public Map<String,String> config;
    @Parameterized.Parameter( 2 )
    public boolean expectFollowersAsReadEndPoints;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> params()
    {
        return Arrays.asList(
                new Object[]{"with followers as read end points",
                        singletonMap( cluster_allow_reads_on_followers.name(), Settings.TRUE ), true},
                new Object[]{"no followers as read end points",
                        singletonMap( cluster_allow_reads_on_followers.name(), Settings.FALSE ), false}
        );
    }

    @Rule
    public final ClusterRule clusterRule = new ClusterRule().withNumberOfCoreMembers( 3 );

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldFindReadWriteAndRouteServers() throws Exception
    {
        // when
        Cluster<?> cluster = clusterRule.withSharedCoreParams( config ).withNumberOfReadReplicas( 1 ).startCluster();

        // then
        int cores = cluster.coreMembers().size();
        int readReplicas = cluster.readReplicas().size();
        int readEndPoints = expectFollowersAsReadEndPoints ? (cores - 1 + readReplicas) : readReplicas;
        for ( int i = 0; i < 3; i++ )
        {
            List<Map<String,Object>> members = getMembers( cluster.getCoreMemberById( i ).database() );

            assertEquals( 1, members.stream().filter( x -> x.get( "role" ).equals( "WRITE" ) )
                    .mapToLong( x -> ((List<String>) x.get( "addresses" )).size() ).sum() );

            assertEquals( readEndPoints, members.stream().filter( x -> x.get( "role" ).equals( "READ" ) )
                    .mapToLong( x -> ((List<String>) x.get( "addresses" )).size() ).sum() );

            assertEquals( cores, members.stream().filter( x -> x.get( "role" ).equals( "ROUTE" ) )
                    .mapToLong( x -> ((List<String>) x.get( "addresses" )).size() ).sum() );
        }
    }

    @Test
    public void shouldNotBeAbleToDiscoverFromReadReplicas() throws Exception
    {
        // given
        Cluster<?> cluster = clusterRule.withSharedCoreParams( config ).withNumberOfReadReplicas( 2 ).startCluster();

        try
        {
            // when
            getMembers( cluster.getReadReplicaById( 0 ).database() );
            fail( "Should not be able to discover members from read replicas" );
        }
        catch ( ProcedureException ex )
        {
            // then
            assertThat( ex.getMessage(), containsString( "There is no procedure with the name" ) );
        }
    }

    @SuppressWarnings( "unchecked" )
    private List<Map<String,Object>> getMembers( GraphDatabaseFacade db ) throws TransactionFailureException,
            ProcedureException
    {
        Kernel kernel = db.getDependencyResolver().resolveDependency( Kernel.class );
        try ( Transaction tx = kernel.beginTransaction( Type.implicit, AnonymousContext.read() ) )
        {
            // when
            List<Object[]> currentMembers =
                    asList( tx.procedures().procedureCallRead( procedureName( GET_SERVERS_V1.fullyQualifiedProcedureName() ), new Object[0],
                            ProcedureCallContext.EMPTY ) );

            return (List<Map<String,Object>>) currentMembers.get( 0 )[1];
        }
    }
}
