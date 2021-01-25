/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import com.neo4j.causalclustering.core.consensus.DurationSinceLastMessageMonitor;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembershipManager;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.state.machines.CommandIndexTracker;
import com.neo4j.causalclustering.discovery.FakeTopologyService;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.monitoring.ThroughputMonitor;
import com.neo4j.dbms.EnterpriseDatabaseState;
import com.neo4j.dbms.EnterpriseOperatorState;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.collection.Dependencies;
import org.neo4j.dbms.StubDatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.monitoring.DatabasePanicEventGenerator;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Health;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.time.FakeClock;

import static com.neo4j.causalclustering.discovery.FakeTopologyService.serverId;
import static com.neo4j.server.rest.causalclustering.ReadReplicaDatabaseEndpointsTest.responseAsMap;
import static java.util.stream.Collectors.toList;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class CoreDatabaseEndpointsTest
{
    private ClusteringEndpoints endpoints;

    private final Dependencies dependencyResolver = new Dependencies();
    private final TestDatabaseIdRepository idRepository = new TestDatabaseIdRepository();
    private final LogProvider logProvider = NullLogProvider.getInstance();
    private final FakeClock clock = new FakeClock();

    // Dependency resolved
    private RaftMembershipManager raftMembershipManager;
    private Health databaseHealth;
    private FakeTopologyService topologyService;
    private DurationSinceLastMessageMonitor raftMessageTimerResetMonitor;
    private RaftMachine raftMachine;
    private CommandIndexTracker commandIndexTracker;
    private ThroughputMonitor throughputMonitor;

    private final ServerId myself = serverId( 0 );
    private final ServerId core2 = serverId( 1 );
    private final ServerId core3 = serverId( 2 );
    private final ServerId replica = serverId( 3 );

    @BeforeEach
    void beforeEach() throws Exception
    {
        var databaseName = DEFAULT_DATABASE_NAME;
        var output = new OutputFormat( new JsonFormat(), new URI( "http://base.local:1234/" ) );
        var managementService = mock( DatabaseManagementService.class );
        var databaseFacade = mock( GraphDatabaseFacade.class );
        when( databaseFacade.databaseName() ).thenReturn( databaseName );
        when( databaseFacade.databaseId() ).thenReturn( idRepository.defaultDatabase() );
        when( databaseFacade.isAvailable( anyLong() ) ).thenReturn( true );
        when( databaseFacade.dbmsInfo() ).thenReturn( DbmsInfo.CORE );
        when( databaseFacade.getDependencyResolver() ).thenReturn( dependencyResolver );
        when( managementService.database( databaseName ) ).thenReturn( databaseFacade );

        raftMembershipManager = dependencyResolver.satisfyDependency( fakeRaftMembershipManager( new HashSet<>( Arrays.asList( myself, core2, core3 ) ) ) );

        databaseHealth = dependencyResolver.satisfyDependency(
                new DatabaseHealth( mock( DatabasePanicEventGenerator.class ), logProvider.getLog( DatabaseHealth.class ) ) );

        topologyService = dependencyResolver.satisfyDependency(
                new FakeTopologyService( Set.of( myself, core2, core3 ), Set.of( replica ), myself, Set.of( idRepository.defaultDatabase() ) ) );

        raftMessageTimerResetMonitor = dependencyResolver.satisfyDependency( new DurationSinceLastMessageMonitor( clock ) );
        raftMachine = dependencyResolver.satisfyDependency( mock( RaftMachine.class ) );
        when( raftMachine.memberId() ).thenReturn( new RaftMemberId( myself.uuid() ) );

        commandIndexTracker = dependencyResolver.satisfyDependency( new CommandIndexTracker() );
        throughputMonitor = dependencyResolver.satisfyDependency( mock( ThroughputMonitor.class ) );

        var stateService = new StubDatabaseStateService( id -> new EnterpriseDatabaseState( id, EnterpriseOperatorState.STARTED ) );

        endpoints = ClusteringDatabaseEndpointsFactory.build( output, stateService, managementService, databaseName, mock( PerDatabaseService.class ) );
    }

    @Test
    void testAnswersWhenLeader()
    {
        // given
        when( raftMachine.currentRole() ).thenReturn( Role.LEADER );

        // when
        var available = endpoints.available();
        var readonly = endpoints.readonly();
        var writable = endpoints.writable();

        // then
        assertEquals( OK.getStatusCode(), available.getStatus() );
        assertEquals( "true", available.getEntity() );

        assertEquals( NOT_FOUND.getStatusCode(), readonly.getStatus() );
        assertEquals( "false", readonly.getEntity() );

        assertEquals( OK.getStatusCode(), writable.getStatus() );
        assertEquals( "true", writable.getEntity() );
    }

    @Test
    void testAnswersWhenCandidate()
    {
        // given
        when( raftMachine.currentRole() ).thenReturn( Role.CANDIDATE );

        // when
        var available = endpoints.available();
        var readonly = endpoints.readonly();
        var writable = endpoints.writable();

        // then
        assertEquals( OK.getStatusCode(), available.getStatus() );
        assertEquals( "true", available.getEntity() );

        assertEquals( OK.getStatusCode(), readonly.getStatus() );
        assertEquals( "true", readonly.getEntity() );

        assertEquals( NOT_FOUND.getStatusCode(), writable.getStatus() );
        assertEquals( "false", writable.getEntity() );
    }

    @Test
    void testAnswersWhenFollower()
    {
        // given
        when( raftMachine.currentRole() ).thenReturn( Role.FOLLOWER );

        // when
        var available = endpoints.available();
        var readonly = endpoints.readonly();
        var writable = endpoints.writable();

        // then
        assertEquals( OK.getStatusCode(), available.getStatus() );
        assertEquals( "true", available.getEntity() );

        assertEquals( OK.getStatusCode(), readonly.getStatus() );
        assertEquals( "true", readonly.getEntity() );

        assertEquals( NOT_FOUND.getStatusCode(), writable.getStatus() );
        assertEquals( "false", writable.getEntity() );
    }

    @Test
    void expectedStatusFieldsAreIncluded() throws IOException
    {
        // given ideal normal conditions
        commandIndexTracker.setAppliedCommandIndex( 123 );
        when( raftMachine.getLeaderInfo() ).thenReturn( Optional.of( new LeaderInfo( new RaftMemberId( core2.uuid() ), 1 ) ) );
        raftMessageTimerResetMonitor.timerReset();
        when( throughputMonitor.throughput() ).thenReturn( Optional.of( 423.0 ) );
        clock.forward( Duration.ofSeconds( 1 ) );

        // and helpers
        var votingMembers = raftMembershipManager.votingMembers()
                .stream()
                .map( memberId -> memberId.uuid().toString() )
                .sorted()
                .collect( toList() );

        // when
        var description = endpoints.description();
        var response = responseAsMap( description );

        // then
        assertThat( response, containsAndEquals( "core", true ) );
        assertThat( response, containsAndEquals( "lastAppliedRaftIndex", 123 ) );
        assertThat( response, containsAndEquals( "participatingInRaftGroup", true ) );
        assertThat( response, containsAndEquals( "votingMembers", votingMembers ) );
        assertThat( response, containsAndEquals( "healthy", true ) );
        assertThat( response, containsAndEquals( "memberId", myself.uuid().toString() ) );
        assertThat( response, containsAndEquals( "leader", core2.uuid().toString() ) );
        assertThat( response, containsAndEquals( "raftCommandsPerSecond", 423.0 ) );
        assertThat( response.toString(), Long.parseLong( response.get( "millisSinceLastLeaderMessage" ).toString() ), greaterThan( 0L ) );
        assertThat( response, containsAndEquals( "discoveryHealthy", true ) );
    }

    @Test
    void notParticipatingInRaftGroupWhenNotInVoterSet() throws IOException
    {
        // given not in voting set
        topologyService.setRole( core2, RoleInfo.LEADER );
        when( raftMembershipManager.votingMembers() ).thenReturn( Stream
                .of( core2, core3 )
                .map( sId -> new RaftMemberId( sId.uuid() ) )
                .collect( Collectors.toSet() ) );

        // when
        var description = endpoints.description();

        // then
        var response = responseAsMap( description );
        assertThat( response, containsAndEquals( "participatingInRaftGroup", false ) );
    }

    @Test
    void notParticipatingInRaftGroupWhenLeaderUnknown() throws IOException
    {
        // given leader is unknown
        topologyService.setRole( null, RoleInfo.LEADER );

        // when
        var description = endpoints.description();

        // then
        var response = responseAsMap( description );
        assertThat( response, containsAndEquals( "participatingInRaftGroup", false ) );
    }

    @Test
    void databaseHealthIsReflected() throws IOException
    {
        // given database is not healthy
        databaseHealth.panic( new RuntimeException() );

        // when
        var description = endpoints.description();
        var response = responseAsMap( description );

        // then
        assertThat( response, containsAndEquals( "healthy", false ) );
    }

    @Test
    void leaderNullWhenUnknown() throws IOException
    {
        // given no leader
        topologyService.setRole( null, RoleInfo.LEADER );

        // when
        var description = endpoints.description();

        // then
        var response = responseAsMap( description );
        assertNull( response.get( "leader" ) );
    }

    @Test
    void throughputNullWhenUnknown() throws IOException
    {
        when( throughputMonitor.throughput() ).thenReturn( Optional.empty() );

        var description = endpoints.description();

        var response = responseAsMap( description );
        assertNull( response.get( "raftCommandsPerSecond" ) );
    }

    private static RaftMembershipManager fakeRaftMembershipManager( Set<ServerId> votingMembers )
    {
        var raftMembershipManager = mock( RaftMembershipManager.class );
        when( raftMembershipManager.votingMembers() ).thenReturn(
                votingMembers.stream().map( sId -> new RaftMemberId( sId.uuid() ) ).collect( Collectors.toSet() ) );
        return raftMembershipManager;
    }

    private static Matcher<Map<String,Object>> containsAndEquals( String key, Object target )
    {
        return new TypeSafeMatcher<>()
        {
            private boolean containsKey;
            private boolean areEqual;

            @Override
            public boolean matchesSafely( Map<String,Object> map )
            {
                if ( !map.containsKey( key ) )
                {
                    return false;
                }
                containsKey = true;
                if ( !map.get( key ).equals( target ) )
                {
                    return false;
                }
                areEqual = true;
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                if ( !containsKey )
                {
                    description.appendText( "did not include key " ).appendValue( key );
                }
                else if ( !areEqual )
                {
                    description.appendText( "key " ).appendValue( key ).appendText( " did not match value" ).appendValue( target );
                }
                else
                {
                    throw new IllegalStateException( "Matcher failed, conditions should have passed" );
                }
            }
        };
    }
}
