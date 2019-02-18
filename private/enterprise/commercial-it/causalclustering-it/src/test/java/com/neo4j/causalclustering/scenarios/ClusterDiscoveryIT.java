/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.test.causalclustering.ClusterRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.configuration.Settings;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.Transaction.Type;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.cluster_allow_reads_on_followers;
import static com.neo4j.causalclustering.routing.load_balancing.procedure.ProcedureNames.GET_SERVERS_V1;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
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
                    .mapToLong( x ->  ((List<?>) x.get( "addresses" ) ).size() ).sum() );

            assertEquals( readEndPoints, members.stream().filter( x -> x.get( "role" ).equals( "READ" ) )
                    .mapToLong( x -> ( (List<?>) x.get( "addresses" )).size() ).sum() );

            assertEquals( cores, members.stream().filter( x -> x.get( "role" ).equals( "ROUTE" ) )
                    .mapToLong( x -> ( (List<?>) x.get( "addresses" ) ).size() ).sum() );
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
            Procedures procedures = tx.procedures();
            int procedureId = procedures.procedureGet( procedureName( GET_SERVERS_V1.fullyQualifiedProcedureName() ) ).id();
            List<AnyValue[]> currentMembers = asList( procedures.procedureCallRead( procedureId, new AnyValue[0] ) );

            ListValue anyValues = (ListValue) currentMembers.get( 0 )[1];
            List<Map<String,Object>> toReturn = new ArrayList<>( anyValues.size() );
            DefaultValueMapper mapper = new DefaultValueMapper( mock( EmbeddedProxySPI.class ) );
            for ( AnyValue anyValue : anyValues )
            {
                MapValue mapValue = (MapValue) anyValue;
                Map<String,Object> map = new HashMap<>();
                mapValue.foreach( ( k, v ) -> map.put( k, v.map( mapper ) ) );
                toReturn.add( map );
            }

            return toReturn;
        }
    }
}
