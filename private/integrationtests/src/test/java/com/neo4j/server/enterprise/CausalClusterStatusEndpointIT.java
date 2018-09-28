/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.CausalClusterInProcessBuilder;
import org.neo4j.harness.PortAuthorityPortPickingStrategy;
import org.neo4j.harness.ServerControls;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.extension.TestDirectoryClassExtension;
import org.neo4j.test.rule.TestDirectory;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Every.everyItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class CausalClusterStatusEndpointIT
{
    @RegisterExtension
    public static TestDirectoryClassExtension testDirectoryClassExtension = new TestDirectoryClassExtension();

    private static final LogProvider LOG_PROVIDER = FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out );

    private static CausalClusterInProcessBuilder.CausalCluster CLUSTER;

    @BeforeAll
    public static void setupClass() throws InterruptedException
    {
        CLUSTER = startCluster( testDirectoryClassExtension.getTestDirectory() );
    }

    @AfterAll
    public static void shutdownClass() throws InterruptedException
    {
        if ( CLUSTER != null )
        {
            CLUSTER.shutdown();
        }
    }

    @Test
    public void leaderIsWritable() throws InterruptedException
    {
        ServerControls leader = getLeader( CLUSTER );
        assertEventually( canVote( leader ), equalTo( true ), 1, TimeUnit.MINUTES );

        String raw = getStatusRaw( getWritableEndpoint( leader.httpURI() ) );
        assertEquals( "true", raw );
    }

    @Test
    public void booleanEndpointsAreReachable() throws InterruptedException
    {
        for ( ServerControls core : CLUSTER.getCoreControls() )
        {
            assertEventually( canVote( core ), equalTo( true ), 1, TimeUnit.MINUTES );

            List<Boolean> availability = Arrays.asList( availabilityStatuses( core.httpURI() ) );
            long trues = availability.stream().filter( i -> i ).count();
            long falses = availability.stream().filter( i -> !i ).count();
            assertEquals( availability.toString(), 1, falses );
            assertEquals( availability.toString(), 2, trues );
        }
    }

    private ThrowingSupplier<Boolean,RuntimeException> canVote( ServerControls core )
    {
        return () -> (Boolean) getStatus( getCcEndpoint( core.httpURI() ) ).get( "participatingInRaftGroup" );
    }

    @Test
    public void statusEndpointIsReachableAndReadable() throws Exception
    {
        // given there is data
        writeSomeData( CLUSTER );
        assertEventually( allReplicaFieldValues( CLUSTER, this::getNodeCount ), everyItem( greaterThan( 1L ) ), 1, TimeUnit.MINUTES );

        // then cores are valid
        CLUSTER.getCoreControls().forEach( member -> assertStatusDescriptionIsValid( member, true ) );

        // and replicas are valid
        CLUSTER.getReplicaControls().forEach( member -> assertStatusDescriptionIsValid( member, false ) );
    }

    @Test
    public void replicasContainTheSameRaftIndexAsCores() throws Exception
    {
        // given starting conditions
        writeSomeData( CLUSTER );
        assertEventually( allReplicaFieldValues( CLUSTER, this::getNodeCount ), allValuesEqual(), 1, TimeUnit.MINUTES );
        long initialLastAppliedRaftIndex =
                Long.parseLong( getStatus( getCcEndpoint( getLeader( CLUSTER ).httpURI() ) ).get( "lastAppliedRaftIndex" ).toString() );
        assertThat( initialLastAppliedRaftIndex, greaterThan( 0L ) );

        // when more data is added
        writeSomeData( CLUSTER );
        assertEventually( allReplicaFieldValues( CLUSTER, this::getNodeCount ), everyItem( greaterThan( 1L ) ), 1, TimeUnit.MINUTES );

        // then all status endpoints have a matching last appliedRaftIndex
        assertEventually( allEndpointsFieldValues( CLUSTER, this::lastAppliedRaftIndex ), allValuesEqual(), 1, TimeUnit.MINUTES );

        // and endpoint last applied raft index has incremented
        long currentLastAppliedRaftIndex =
                Long.parseLong( getStatus( getCcEndpoint( getLeader( CLUSTER ).httpURI() ) ).get( "lastAppliedRaftIndex" ).toString() );
        assertThat( currentLastAppliedRaftIndex, greaterThan( initialLastAppliedRaftIndex ) );
    }

    private static <T> Matcher<Collection<T>> allValuesEqual()
    {
        return new BaseMatcher<Collection<T>>()
        {
            @Override
            public boolean matches( Object item )
            {
                Collection<T> castedItem = (Collection<T>) item;
                return castedItem.stream().distinct().count() == 1;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "Values should be equal" );
            }
        };
    }

    private Long getNodeCount( ServerControls serverControls )
    {
        GraphDatabaseService db = serverControls.graph();
        long count;
        try ( Transaction tx = db.beginTx() )
        {
            count = db.getAllNodes().stream().collect( Collectors.toList() ).size();
        }
        return count;
    }

    private Long lastAppliedRaftIndex( ServerControls serverControls )
    {
        return Long.parseLong( getStatus( getCcEndpoint( serverControls.httpURI() ) ).get( "lastAppliedRaftIndex" ).toString() );
    }

    private static <T> ThrowingSupplier<Collection<T>,RuntimeException> allEndpointsFieldValues( CausalClusterInProcessBuilder.CausalCluster cluster,
            Function<ServerControls,T> mapper )
    {
        return () -> Stream.of( cluster.getCoreControls(), cluster.getReplicaControls() )
                .flatMap( Collection::stream )
                .map( mapper )
                .collect( Collectors.toList() );
    }

    private static <T> ThrowingSupplier<Collection<T>,RuntimeException> allReplicaFieldValues( CausalClusterInProcessBuilder.CausalCluster cluster,
            Function<ServerControls,T> mapper )
    {
        return () -> cluster.getReplicaControls().stream().map( mapper ).collect( Collectors.toList() );
    }

    private void assertStatusDescriptionIsValid( ServerControls member, boolean isCore )
    {
        Map<String,Object> statusDescription = getStatus( getCcEndpoint( member.httpURI() ) );
        String msg = statusDescription.toString();
        assertEquals( msg, isCore, Boolean.parseBoolean( statusDescription.get( "core" ).toString() ) );
        assertThat( msg, Long.valueOf( statusDescription.get( "lastAppliedRaftIndex" ).toString() ), greaterThan( 0L ) );
        assertVotingMembers( statusDescription );
        assertParticipatingInRaftGroup( statusDescription, isCore );
        assertMillisSinceLastLeaderMessage( statusDescription, isCore );
        assertTrue( msg, Boolean.valueOf( statusDescription.get( "healthy" ).toString() ) );
        assertTrue( msg, StringUtils.isNotEmpty( statusDescription.get( "memberId" ).toString() ) );
        assertFalse( msg, Boolean.valueOf( statusDescription.get( "leader" ).toString() ) );
    }

    private static void assertVotingMembers( Map<String,Object> statusDescription )
    {
        List<String> members = (List<String>) statusDescription.get( "votingMembers" );
        assertEquals( statusDescription.toString(), 3, members.size() );
    }

    private static void assertParticipatingInRaftGroup( Map<String,Object> statusDescription, boolean isCore )
    {
        assertNotNull( statusDescription.get( "participatingInRaftGroup" ) );
        boolean participatingInRaftGroup = Boolean.parseBoolean( statusDescription.get( "participatingInRaftGroup" ).toString() );
        if ( isCore )
        {
            assertTrue( participatingInRaftGroup );
        }
        else
        {
            assertFalse( participatingInRaftGroup );
        }
    }

    private static void assertMillisSinceLastLeaderMessage( Map<String,Object> statusDescription, boolean isCore )
    {
        Object millisSinceLastLeaderMessage = statusDescription.get( "millisSinceLastLeaderMessage" );
        if ( isCore )
        {
            assertThat( statusDescription.toString(), Long.parseLong( millisSinceLastLeaderMessage.toString() ), greaterThan( 0L ) );
        }
        else
        {
            assertNull( millisSinceLastLeaderMessage );
        }
    }

    private void writeSomeData( CausalClusterInProcessBuilder.CausalCluster cluster )
    {
        GraphDatabaseService db = getLeader( cluster ).graph();
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( Label.label( "MyNode" ) );
            tx.success();
        }
    }

    private static CausalClusterInProcessBuilder.CausalCluster startCluster( TestDirectory testDirectory ) throws InterruptedException
    {
        File clusterDirectory = testDirectory.directory( "CLUSTER" );
        CausalClusterInProcessBuilder.CausalCluster cluster = CausalClusterInProcessBuilder.init()
                .withCores( 3 )
                .withReplicas( 2 )
                .withLogger( LOG_PROVIDER )
                .atPath( clusterDirectory.toPath() )
                .withOptionalPortsStrategy( new PortAuthorityPortPickingStrategy() )
                .build();

        cluster.boot();
        return cluster;
    }

    private ServerControls getLeader( CausalClusterInProcessBuilder.CausalCluster cluster )
    {
        return cluster.getCoreControls()
                .stream()
                .filter( core -> Role.LEADER.equals( ((CoreGraphDatabase) core.graph()).getRole() ) )
                .findAny()
                .orElseThrow( () -> new IllegalStateException( "Leader does not exist" ) );
    }

    private Boolean[] availabilityStatuses( URI server )
    {
        Boolean writable = Boolean.parseBoolean( getStatusRaw( getWritableEndpoint( server ) ) );
        Boolean readonly = Boolean.parseBoolean( getStatusRaw( getReadOnlyEndpoint( server ) ) );
        Boolean availability = Boolean.parseBoolean( getStatusRaw( getAvailability( server ) ) );
        return new Boolean[]{writable, readonly, availability};
    }

    private static String getStatusRaw( String address )
    {
        return getStatusRaw( address, null );
    }

    private static String getStatusRaw( String address, Integer expectedStatus )
    {
        Client client = Client.create();
        ClientResponse r = client.resource( address ).accept( APPLICATION_JSON ).get( ClientResponse.class );
        if ( expectedStatus != null )
        {
            assertEquals( expectedStatus.intValue(), r.getStatus() );
        }
        return r.getEntity( String.class );
    }

    private static Map<String,Object> getStatus( String address )
    {
        ObjectMapper objectMapper = new ObjectMapper();
        String raw = getStatusRaw( address );
        try
        {
            return objectMapper.readValue( raw, new TypeReference<Map<String,Object>>()
            {
            } );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static String getReadOnlyEndpoint( URI server )
    {
        return endpointFromServer( server, "/db/manage/server/causalclustering/read-only" );
    }

    private static String getWritableEndpoint( URI server )
    {
        return endpointFromServer( server, "/db/manage/server/causalclustering/writable" );
    }

    private static String getAvailability( URI server )
    {
        return endpointFromServer( server, "/db/manage/server/causalclustering/available" );
    }

    private static String getCcEndpoint( URI server )
    {
        return endpointFromServer( server, "/db/manage/server/causalclustering/status" );
    }

    private static String endpointFromServer( URI server, String endpoint )
    {
        return (server.toString() + endpoint).replaceAll( "//", "/" ).replaceAll( ":/", "://" );
    }
}
