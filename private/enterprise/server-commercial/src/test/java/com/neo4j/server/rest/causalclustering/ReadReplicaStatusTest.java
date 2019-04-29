/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import com.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.monitoring.ThroughputMonitor;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.IntStream;
import javax.ws.rs.core.Response;

import org.neo4j.collection.Dependencies;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.DatabasePanicEventGenerator;
import org.neo4j.monitoring.Health;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.database.Database;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.time.FakeClock;
import org.neo4j.time.SystemNanoClock;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class ReadReplicaStatusTest
{
    private CausalClusteringStatus status;

    private FakeTopologyService topologyService;
    private Dependencies dependencyResolver = new Dependencies();
    private Health databaseHealth;
    private CommandIndexTracker commandIndexTracker;

    private final MemberId myself = new MemberId( UUID.randomUUID() );
    private final LogProvider logProvider = NullLogProvider.getInstance();
    private final SystemNanoClock clock = new FakeClock();

    @BeforeEach
    void setup() throws Exception
    {
        OutputFormat output = new OutputFormat( new JsonFormat(), new URI( "http://base.local:1234/" ) );
        Database database = mock( Database.class );
        GraphDatabaseFacade db = mock( GraphDatabaseFacade.class );
        when( database.getGraph() ).thenReturn( db );
        topologyService = new FakeTopologyService( randomMembers( 3 ), randomMembers( 2 ), myself, RoleInfo.READ_REPLICA );
        dependencyResolver.satisfyDependency( DatabaseInfo.READ_REPLICA );
        dependencyResolver.satisfyDependencies( topologyService );
        JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        dependencyResolver.satisfyDependency( jobScheduler );

        when( db.getDependencyResolver() ).thenReturn( dependencyResolver );
        databaseHealth = dependencyResolver.satisfyDependency(
                new DatabaseHealth( mock( DatabasePanicEventGenerator.class ), logProvider.getLog( DatabaseHealth.class ) ) );
        commandIndexTracker = dependencyResolver.satisfyDependency( new CommandIndexTracker() );
        dependencyResolver.satisfyDependency(
                new ThroughputMonitor( logProvider, clock, jobScheduler, Duration.of( 5, SECONDS ), commandIndexTracker::getAppliedCommandIndex ) );

        org.neo4j.kernel.database.Database internalDatabase = mock( org.neo4j.kernel.database.Database.class );
        when( internalDatabase.getDatabaseId() ).thenReturn( new DatabaseId( DEFAULT_DATABASE_NAME ) );
        dependencyResolver.satisfyDependency( internalDatabase );

        status = CausalClusteringStatusFactory.build( output, database );
    }

    @AfterEach
    void cleanUp() throws Exception
    {
        dependencyResolver.resolveDependency( JobScheduler.class ).shutdown();
    }

    @Test
    void testAnswers()
    {
        // when
        Response available = status.available();
        Response readonly = status.readonly();
        Response writable = status.writable();

        // then
        assertEquals( OK.getStatusCode(), available.getStatus() );
        assertEquals( "true", available.getEntity() );

        assertEquals( OK.getStatusCode(), readonly.getStatus() );
        assertEquals( "true", readonly.getEntity() );

        assertEquals( NOT_FOUND.getStatusCode(), writable.getStatus() );
        assertEquals( "false", writable.getEntity() );
    }

    @Test
    void statusIncludesAppliedRaftLogIndex() throws IOException
    {
        // given
        commandIndexTracker.setAppliedCommandIndex( 321 );

        // when
        Response description = status.description();

        // then
        Map<String,Object> responseJson = responseAsMap( description );
        assertEquals( 321, responseJson.get( "lastAppliedRaftIndex" ) );
    }

    @Test
    void responseIncludesAllCores() throws IOException
    {
        Response description = status.description();

        assertEquals( Response.Status.OK.getStatusCode(), description.getStatus() );
        List<String> expectedVotingMembers = topologyService.allCoreServers()
                .keySet()
                .stream()
                .map( memberId -> memberId.getUuid().toString() )
                .collect( toList() );
        Map<String,Object> responseJson = responseAsMap( description );
        List<String> actualVotingMembers = (List<String>) responseJson.get( "votingMembers" );
        Collections.sort( expectedVotingMembers );
        Collections.sort( actualVotingMembers );
        assertEquals( expectedVotingMembers, actualVotingMembers );
    }

    @Test
    void dbHealthIsIncludedInResponse() throws IOException
    {
        Response description = status.description();
        assertEquals( true, responseAsMap( description ).get( "healthy" ) );

        databaseHealth.panic( new RuntimeException() );
        description = status.description();
        assertEquals( false, responseAsMap( description ).get( "healthy" ) );
    }

    @Test
    void includesMemberId() throws IOException
    {
        Response description = status.description();
        assertEquals( myself.getUuid().toString(), responseAsMap( description ).get( "memberId" ) );
    }

    @Test
    void leaderIsNullWhenUnknown() throws IOException
    {
        Response description = status.description();
        assertNull( responseAsMap( description ).get( "leader" ) );

        MemberId selectedLead = topologyService.allCoreServers()
                .keySet()
                .stream()
                .findFirst()
                .orElseThrow( () -> new IllegalStateException( "No cores in topology" ) );
        topologyService.replaceWithRole( selectedLead, RoleInfo.LEADER );
        description = status.description();
        assertEquals( selectedLead.getUuid().toString(), responseAsMap( description ).get( "leader" ) );
    }

    @Test
    void isNotCore() throws IOException
    {
        Response description = status.description();
        assertTrue( responseAsMap( description ).containsKey( "core" ) );
        assertEquals( false, responseAsMap( status.description() ).get( "core" ) );
    }

    @Test
    void throughputNullWhenUnknown() throws IOException
    {
        ThroughputMonitor throughputMonitor = mock( ThroughputMonitor.class );
        when( throughputMonitor.throughput() ).thenReturn( Optional.empty() );
        dependencyResolver.satisfyDependency( throughputMonitor );

        Response description = status.description();

        Map<String,Object> response = responseAsMap( description );
        assertNull( response.get( "raftCommandsPerSecond" ) );
    }

    static Collection<MemberId> randomMembers( int size )
    {
        return IntStream.range( 0, size )
                .mapToObj( i -> UUID.randomUUID() )
                .map( MemberId::new )
                .collect( toList() );
    }

    static Map<String,Object> responseAsMap( Response response ) throws IOException
    {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue( response.getEntity().toString(), new TypeReference<Map<String,Object>>()
        {
        } );
    }
}
