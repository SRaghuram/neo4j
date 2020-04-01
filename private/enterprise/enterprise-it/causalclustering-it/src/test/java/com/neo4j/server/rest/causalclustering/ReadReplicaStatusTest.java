/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.neo4j.causalclustering.core.state.machines.CommandIndexTracker;
import com.neo4j.causalclustering.discovery.FakeTopologyService;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.monitoring.ThroughputMonitorService;
import com.neo4j.dbms.EnterpriseOperatorState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.core.Response;

import org.neo4j.collection.Dependencies;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.DatabasePanicEventGenerator;
import org.neo4j.monitoring.Health;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.time.FakeClock;
import org.neo4j.time.SystemNanoClock;

import static com.neo4j.causalclustering.discovery.FakeTopologyService.memberId;
import static com.neo4j.causalclustering.discovery.FakeTopologyService.memberIds;
import static java.util.stream.Collectors.toList;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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

    private final TestDatabaseIdRepository idRepository = new TestDatabaseIdRepository();
    private final Set<MemberId> cores = memberIds( 0,3 );
    private final Set<MemberId> replicas = memberIds( 3, 5 );
    private final MemberId myself = memberId( 3 );
    private final LogProvider logProvider = NullLogProvider.getInstance();
    private final SystemNanoClock clock = new FakeClock();

    @BeforeEach
    void setup() throws Exception
    {
        var databaseName = DEFAULT_DATABASE_NAME;
        var output = new OutputFormat( new JsonFormat(), new URI( "http://base.local:1234/" ) );
        var managementService = mock( DatabaseManagementService.class );
        var db = mock( GraphDatabaseFacade.class );
        when( db.databaseName() ).thenReturn( databaseName );
        when( db.databaseId() ).thenReturn( idRepository.defaultDatabase() );
        when( db.databaseInfo() ).thenReturn( DatabaseInfo.READ_REPLICA );
        when( managementService.database( databaseName ) ).thenReturn( db );
        topologyService = new FakeTopologyService( cores, replicas, myself, Set.of( idRepository.defaultDatabase() ) );
        dependencyResolver.satisfyDependencies( topologyService );
        var jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        dependencyResolver.satisfyDependency( jobScheduler );

        when( db.getDependencyResolver() ).thenReturn( dependencyResolver );
        databaseHealth = dependencyResolver.satisfyDependency(
                new DatabaseHealth( mock( DatabasePanicEventGenerator.class ), logProvider.getLog( DatabaseHealth.class ) ) );
        commandIndexTracker = dependencyResolver.satisfyDependency( new CommandIndexTracker() );
        var throughputMonitor = new ThroughputMonitorService( clock, jobScheduler, Duration.ofSeconds( 5 ), logProvider ).createMonitor( commandIndexTracker );
        dependencyResolver.satisfyDependency( throughputMonitor );

        var internalDatabase = mock( org.neo4j.kernel.database.Database.class );
        when( internalDatabase.getNamedDatabaseId() ).thenReturn( new TestDatabaseIdRepository().defaultDatabase() );
        dependencyResolver.satisfyDependency( internalDatabase );

        var databaseStateService = mock( DatabaseStateService.class );
        when( databaseStateService.stateOfDatabase( any( NamedDatabaseId.class ) ) ).thenReturn( EnterpriseOperatorState.STARTED );

        status = CausalClusteringStatusFactory.build( output, databaseStateService, managementService, DEFAULT_DATABASE_NAME, mock( ClusterService.class ) );
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
        var available = status.available();
        var readonly = status.readonly();
        var writable = status.writable();

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
        var description = status.description();

        // then
        var responseJson = responseAsMap( description );
        assertEquals( 321, responseJson.get( "lastAppliedRaftIndex" ) );
    }

    @Test
    void responseIncludesAllCores() throws IOException
    {
        var description = status.description();

        assertEquals( Response.Status.OK.getStatusCode(), description.getStatus() );
        var expectedVotingMembers = topologyService.allCoreServers()
                .keySet()
                .stream()
                .map( memberId -> memberId.getUuid().toString() )
                .collect( toList() );
        var responseJson = responseAsMap( description );
        //noinspection unchecked
        var actualVotingMembers = (List<String>) responseJson.get( "votingMembers" );
        Collections.sort( expectedVotingMembers );
        Collections.sort( actualVotingMembers );
        assertEquals( expectedVotingMembers, actualVotingMembers );
    }

    @Test
    void dbHealthIsIncludedInResponse() throws IOException
    {
        var description = status.description();
        assertEquals( true, responseAsMap( description ).get( "healthy" ) );

        databaseHealth.panic( new RuntimeException() );
        description = status.description();
        assertEquals( false, responseAsMap( description ).get( "healthy" ) );
    }

    @Test
    void includesMemberId() throws IOException
    {
        var description = status.description();
        assertEquals( myself.getUuid().toString(), responseAsMap( description ).get( "memberId" ) );
    }

    @Test
    void leaderIsNullWhenUnknown() throws IOException
    {
        topologyService.setRole( null, RoleInfo.LEADER );
        var description = status.description();
        assertNull( responseAsMap( description ).get( "leader" ) );

        var selectedLead = topologyService.allCoreServers()
                .keySet()
                .stream()
                .findFirst()
                .orElseThrow( () -> new IllegalStateException( "No cores in topology" ) );
        topologyService.setRole( selectedLead, RoleInfo.LEADER );
        description = status.description();
        assertEquals( selectedLead.getUuid().toString(), responseAsMap( description ).get( "leader" ) );
    }

    @Test
    void isNotCore() throws IOException
    {
        var description = status.description();
        assertTrue( responseAsMap( description ).containsKey( "core" ) );
        assertEquals( false, responseAsMap( status.description() ).get( "core" ) );
    }

    @Test
    void throughputNullWhenUnknown() throws IOException
    {
        var description = status.description();

        var response = responseAsMap( description );
        assertNull( response.get( "raftCommandsPerSecond" ) );
    }

    static Map<String,Object> responseAsMap( Response response ) throws IOException
    {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue( response.getEntity().toString(), new TypeReference<>()
        {
        } );
    }
}
