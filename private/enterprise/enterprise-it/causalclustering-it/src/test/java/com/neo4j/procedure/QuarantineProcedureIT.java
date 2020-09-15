/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.dbms.ShowDatabasesHelpers;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.OperatorState;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.test.assertion.Assert;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStopped;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.showDatabases;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.startDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.stopDatabase;
import static com.neo4j.dbms.EnterpriseOperatorState.QUARANTINED;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@ClusterExtension
class QuarantineProcedureIT
{
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeAll
    void startCluster() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig().withNumberOfReadReplicas( 0 ) );
        cluster.start();
        cluster.awaitLeader( SYSTEM_DATABASE_NAME );
    }

    @Test
    void placeDatabaseIntoQuarantine() throws Exception
    {
        // given
        var databaseName = TestDatabaseIdRepository.randomNamedDatabaseId().name();
        var reason = "Just a reason";
        var differentReason = "Different reason";
        createDatabase( databaseName, cluster );
        assertDatabaseEventuallyStarted( databaseName, cluster );
        var member = cluster.randomCoreMember( true ).get();

        // when
        var result = callQuarantine( member, databaseName, true, reason );

        // then
        assertQuarantinedState( result, member, databaseName, reason );

        // when
        var secondResult = callQuarantine( member, databaseName, true, differentReason );
        assertTrue( secondResult.contains( result ) );
        assertFalse( secondResult.contains( differentReason ) );
    }

    @Test
    void removeDatabaseFromQuarantine() throws Exception
    {
        // given
        var databaseName = TestDatabaseIdRepository.randomNamedDatabaseId().name();
        var reason = "Just a reason";
        createDatabase( databaseName, cluster );
        assertDatabaseEventuallyStarted( databaseName, cluster );
        var member = cluster.randomCoreMember( true ).get();
        var databaseStateService = member.systemDatabase().getDependencyResolver().resolveDependency( DatabaseStateService.class );
        var databaseId = member.databaseId( databaseName );
        var originalState = databaseStateService.stateOfDatabase( databaseId );
        var result = callQuarantine( member, databaseName, true, reason );
        assertQuarantinedState( result, member, databaseName, reason );

        // when
        var secondResult = callQuarantine( member, databaseName, false, "ignored" );

        // then
        assertEquals( result, secondResult );
        assertDatabaseState( member, originalState, databaseName, "" );
        assertTrue( databaseStateService.causeOfFailure( databaseId ).isEmpty() );
        assertFalse( testDirectory.getFileSystem().fileExists( member.clusterStateLayout().quarantineMarkerStateFile( databaseName ) ) );
    }

    @Test
    void stoppedDatabaseShouldRemainStopped() throws Exception
    {
        // given
        var databaseName = TestDatabaseIdRepository.randomNamedDatabaseId().name();
        var reason = "Just a reason";
        createDatabase( databaseName, cluster );
        assertDatabaseEventuallyStarted( databaseName, cluster );
        stopDatabase( databaseName, cluster );
        assertDatabaseEventuallyStopped( databaseName, cluster );
        var member = cluster.randomCoreMember( true ).get();
        var databaseStateService = member.systemDatabase().getDependencyResolver().resolveDependency( DatabaseStateService.class );
        var databaseId = member.databaseId( databaseName );

        // when
        var result = callQuarantine( member, databaseName, true, reason );

        // then
        assertQuarantinedState( result, member, databaseName, reason );

        // when
        startDatabase( databaseName, cluster );
        var others = cluster.allMembers();
        others.remove( member );
        // then
        assertDatabaseEventuallyStarted( databaseName, others );
        assertQuarantinedState( result, member, databaseName, reason );

        // when
        callQuarantine( member, databaseName, false, "ignored" );

        // then
        assertDatabaseState( member, STARTED, databaseName, "" );
    }

    @Test
    void quarantineMarkerShouldSurviveRestart() throws Exception
    {
        // given
        var databaseName = TestDatabaseIdRepository.randomNamedDatabaseId().name();
        var reason = "Just a reason";
        var member = cluster.randomCoreMember( true ).get();
        createDatabase( databaseName, cluster );
        assertDatabaseEventuallyStarted( databaseName, cluster );
        var result = callQuarantine( member, databaseName, true, reason );
        assertQuarantinedState( result, member, databaseName, reason );

        // when
        cluster.shutdown();
        cluster.start();
        cluster.awaitLeader( SYSTEM_DATABASE_NAME );

        // then
        assertQuarantinedState( result, member, databaseName, reason );
    }

    private static String callQuarantine( CoreClusterMember member, String databaseName, boolean setStatus, String reason )
    {
        return member.systemDatabase().executeTransactionally( "CALL dbms.cluster.quarantineDatabase($db,$setStatus,$reason)",
                Map.of( "db", databaseName, "setStatus", setStatus, "reason", reason ), rawResult ->
                {
                    var resultRows = Iterators.asList( rawResult );
                    assertEquals( 1, resultRows.size() );
                    var resultRow = resultRows.get( 0 );
                    assertEquals( databaseName, resultRow.get( "databaseName" ) );
                    assertEquals( setStatus, resultRow.get( "quarantined" ) );
                    return (String) resultRow.get( "result" );
                } );
    }

    private void assertQuarantinedState( String result, CoreClusterMember member, String databaseName, String reason )
    {
        var databaseStateService = member.systemDatabase().getDependencyResolver().resolveDependency( DatabaseStateService.class );
        var databaseId = member.databaseId( databaseName );
        assertTrue( result.contains( reason ) );
        assertDatabaseState( member, QUARANTINED, databaseName, reason );

        var failure = databaseStateService.causeOfFailure( databaseId );
        assertTrue( failure.isPresent() );
        assertEquals( result, failure.get().getMessage() );
        assertTrue( testDirectory.getFileSystem().fileExists( member.clusterStateLayout().quarantineMarkerStateFile( databaseName ) ) );
    }

    private void assertDatabaseState( CoreClusterMember member, OperatorState operatorState, String databaseName, String reason )
    {
        var databaseStateService = member.systemDatabase().getDependencyResolver().resolveDependency( DatabaseStateService.class );
        var databaseId = member.databaseId( databaseName );
        assertEventually( () -> databaseStateService.stateOfDatabase( databaseId ), state -> operatorState == state );
        assertEventually( () -> showDatabases( cluster ), list -> showDatabasesMatch( list, databaseName, member, operatorState, reason ) );
    }

    private static <T> void assertEventually( Callable<T> actual, Predicate<? super T> condition )
    {
        Assert.assertEventually( actual, condition, 10, SECONDS );
    }

    private static boolean showDatabasesMatch( List<ShowDatabasesHelpers.ShowDatabasesResultRow> list, String databaseName,
            CoreClusterMember member, OperatorState state, String reason )
    {
        var filteredList = list.stream()
                .filter( element -> element.name().equals( databaseName ) )
                .filter( element -> element.address().equals( member.boltAdvertisedAddress() ) )
                .collect( Collectors.toList() );
        if ( filteredList.size() != 1 )
        {
            return false;
        }
        var exactRow = filteredList.get( 0 );
        return state.description().equals( exactRow.currentStatus() ) && exactRow.error().endsWith( reason );
    }
}
