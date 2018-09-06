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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.CausalClusterInProcessBuilder;
import org.neo4j.harness.PortAuthorityPortPickingStrategy;
import org.neo4j.harness.ServerControls;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@ExtendWith( {TestDirectoryExtension.class} )
public class CausalClusterStatusEndpointIT
{
    @Inject
    private TestDirectory testDirectory;

    private static final LogProvider LOG_PROVIDER = FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out );

    private static CausalClusterInProcessBuilder.CausalCluster startCluster( TestDirectory testDirectory ) throws InterruptedException
    {
        File clusterDirectory = testDirectory.directory( "cluster" );
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

    @Test
    public void leaderIsWritable() throws InterruptedException
    {
        CausalClusterInProcessBuilder.CausalCluster cluster = null;
        try
        {
            cluster = startCluster( testDirectory );
            ServerControls leader = getLeader( cluster );
            waitUntilCanVote( leader.httpURI() );

            String raw = getStatusRaw( getWritableEndpoint( leader.httpURI() ) );
            assertEquals( "true", raw );
        }
        finally
        {
            if ( cluster != null )
            {
                cluster.shutdown();
            }
        }
    }

    @Test
    public void booleanEndpointsAreReachable() throws InterruptedException
    {
        CausalClusterInProcessBuilder.CausalCluster cluster = null;
        try
        {
            cluster = startCluster( testDirectory );
            for ( ServerControls core : cluster.getCoreControls() )
            {
                waitUntilCanVote( core.httpURI() );

                List<Boolean> availability = Arrays.asList( availabilityStatuses( core.httpURI() ) );
                long trues = availability.stream().filter( i -> i ).count();
                long falses = availability.stream().filter( i -> !i ).count();
                assertEquals( availability.toString(), 1, falses );
                assertEquals( availability.toString(), 2, trues );
            }
        }
        finally
        {
            if ( cluster != null )
            {
                cluster.shutdown();
            }
        }
    }

    @Test
    public void statusEndpointIsReachableAndReadable() throws Exception
    {
        CausalClusterInProcessBuilder.CausalCluster cluster = null;
        try
        {
            // given a running cluster
            cluster = startCluster( testDirectory );

            // and there is data
            writeSomeData( cluster );
            waitUntilCondition( allReplicasMatch( cluster, nodeCount( 1 ) ), Duration.ofMinutes( 1 ), Duration.ofSeconds( 1 ) );

            // then cores are valid
            cluster.getCoreControls().stream().forEach( member -> assertStatusDescriptionIsValid( member, true ) );

            // and replicas are valid
            cluster.getReplicaControls().stream().forEach( member -> assertStatusDescriptionIsValid( member, false ) );
        }
        finally
        {
            if ( cluster != null )
            {
                cluster.shutdown();
            }
        }
    }

    @Test
    public void replicasContainTheSameRaftIndexAsCores() throws Exception
    {
        CausalClusterInProcessBuilder.CausalCluster cluster = null;
        try
        {
            //given
            cluster = startCluster( testDirectory );

            // and some data
            writeSomeData( cluster );
            waitUntilCondition( allReplicasMatch( cluster, nodeCount( 1 ) ), Duration.ofMinutes( 1 ), Duration.ofSeconds( 1 ) );

            // when all status endpoints are accessed for last applied raft index
            List<Long> allCoreLastAppliedRaftIndex = cluster.getCoreControls()
                    .stream()
                    .map( ServerControls::httpURI )
                    .map( CausalClusterStatusEndpointIT::getCcEndpoint )
                    .map( CausalClusterStatusEndpointIT::getStatus )
                    .map( responseMap -> responseMap.get( "lastAppliedRaftIndex" ).toString() )
                    .map( Long::valueOf )
                    .collect( Collectors.toList() );
            List<Long> replicaLastAppliedRaftIndex = cluster.getReplicaControls()
                    .stream()
                    .map( ServerControls::httpURI )
                    .map( CausalClusterStatusEndpointIT::getCcEndpoint )
                    .map( CausalClusterStatusEndpointIT::getStatus )
                    .map( responseMap -> responseMap.get( "lastAppliedRaftIndex" ).toString() )
                    .map( Long::valueOf )
                    .collect( Collectors.toList() );

            // then
            for ( Long core : allCoreLastAppliedRaftIndex )
            {
                assertEquals( allCoreLastAppliedRaftIndex.get( 0 ), core );
            }
            for ( Long replica : replicaLastAppliedRaftIndex )
            {
                assertEquals( allCoreLastAppliedRaftIndex.get( 0 ), replica );
            }
            assertThat( allCoreLastAppliedRaftIndex.get( 0 ), greaterThan( 0L ) );
        }
        finally
        {
            if ( cluster != null )
            {
                cluster.shutdown();
            }
        }
    }

    private Supplier<Boolean> allReplicasMatch( CausalClusterInProcessBuilder.CausalCluster cluster, Function<ServerControls,Boolean> condition )
    {
        return () -> cluster.getReplicaControls().stream().map( condition ).reduce( Boolean::logicalAnd ).orElse( false );
    }

    private Function<ServerControls,Boolean> nodeCount( int numberOfNodes )
    {
        return server ->
        {
            GraphDatabaseService db = server.graph();
            int count;
            try ( Transaction tx = db.beginTx() )
            {
                count = db.getAllNodes().stream().collect( Collectors.toList() ).size();
            }
            return numberOfNodes == count;
        };
    }

    private void waitUntilCondition( Supplier<Boolean> condition, Duration timeout, Duration waitPeriod ) throws InterruptedException
    {
        LocalDateTime startTime = LocalDateTime.now();
        while ( LocalDateTime.now().isBefore( startTime.plus( timeout ) ) )
        {
            if ( condition.get() )
            {
                return;
            }
            Thread.sleep( waitPeriod.toMillis() );
        }
        throw new IllegalStateException( "Condition not met within " + timeout );
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

    private static void waitUntilCanVote( URI server )
    {
        Supplier<Boolean> canVote = () ->
        {
            Map<String,Object> statusDescription = getStatus( getCcEndpoint( server ) );
            return (Boolean) statusDescription.get( "participatingInRaftGroup" );
        };
        repeatUntilCondition( canVote, 10, 500 );
    }

    private static void repeatUntilCondition( Supplier<Boolean> condition, int times, long millis )
    {
        for ( int i = 0; i < times; i++ )
        {
            if ( condition.get() )
            {
                return;
            }
            try
            {
                Thread.sleep( millis );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }
        throw new IllegalStateException( format( "Failed to pass condition %d times with %d ms breaks", times, millis ) );
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
