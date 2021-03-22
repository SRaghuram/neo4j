/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.neo4j.causalclustering.discovery.FakeTopologyService;
import com.neo4j.dbms.EnterpriseDatabaseState;
import com.neo4j.dbms.EnterpriseOperatorState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.core.Response;

import org.neo4j.collection.Dependencies;
import org.neo4j.dbms.StubDatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.monitoring.DatabasePanicEventGenerator;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Health;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;

import static com.neo4j.causalclustering.discovery.FakeTopologyService.serverId;
import static com.neo4j.causalclustering.discovery.FakeTopologyService.serverIds;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class StandaloneDatabaseEndpointsTest
{
    private ClusteringEndpoints status;

    private Dependencies dependencyResolver = new Dependencies();
    private Health databaseHealth;

    private final TestDatabaseIdRepository idRepository = new TestDatabaseIdRepository();
    private final ServerId myself = serverId( 0 );
    private final Set<ServerId> replicas = serverIds( 3, 5 );
    private final LogProvider logProvider = NullLogProvider.getInstance();

    @BeforeEach
    void setup() throws Exception
    {
        var databaseName = DEFAULT_DATABASE_NAME;
        var output = new OutputFormat( new JsonFormat(), new URI( "http://base.local:1234/" ) );
        var managementService = mock( DatabaseManagementService.class );
        var db = mock( GraphDatabaseFacade.class );
        when( db.databaseName() ).thenReturn( databaseName );
        when( db.databaseId() ).thenReturn( idRepository.defaultDatabase() );
        when( db.isAvailable( anyLong() ) ).thenReturn( true );
        when( db.dbmsInfo() ).thenReturn( DbmsInfo.ENTERPRISE );
        when( managementService.database( databaseName ) ).thenReturn( db );
        var topologyService = new FakeTopologyService( Set.of( myself ), replicas, myself, Set.of( idRepository.defaultDatabase() ) );
        dependencyResolver.satisfyDependencies( topologyService );
        var jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        dependencyResolver.satisfyDependency( jobScheduler );

        when( db.getDependencyResolver() ).thenReturn( dependencyResolver );
        databaseHealth = dependencyResolver.satisfyDependency(
                new DatabaseHealth( mock( DatabasePanicEventGenerator.class ), logProvider.getLog( DatabaseHealth.class ) ) );

        var stateService = new StubDatabaseStateService( id -> new EnterpriseDatabaseState( id, EnterpriseOperatorState.STARTED ) );

        status = ClusteringDatabaseEndpointsFactory.build( output, stateService, managementService,
                                                           DEFAULT_DATABASE_NAME, mock( PerDatabaseService.class ) );
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

        assertEquals( NOT_FOUND.getStatusCode(), readonly.getStatus() );
        assertEquals( "false", readonly.getEntity() );

        assertEquals( OK.getStatusCode(), writable.getStatus() );
        assertEquals( "true", writable.getEntity() );
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
        assertEquals( myself.uuid().toString(), responseAsMap( description ).get( "memberId" ) );
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
