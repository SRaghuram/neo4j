/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import com.neo4j.test.driver.DriverExtension;
import com.neo4j.test.driver.DriverFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.test.extension.Inject;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.driver.SessionConfig.forDatabase;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@ClusterExtension
@DriverExtension
@TestInstance( TestInstance.Lifecycle.PER_METHOD )
class PanicIT
{
    private static final int INITIAL_CORE_MEMBERS = 3;
    private static final int INITIAL_READ_REPLICAS = 1;

    @Inject
    private ClusterFactory clusterFactory;

    @Inject
    private DriverFactory driverFactory;

    private Cluster cluster;

    @BeforeEach
    void startCluster() throws ExecutionException, InterruptedException
    {
        cluster = clusterFactory.createCluster(
                ClusterConfig.clusterConfig().withNumberOfReadReplicas( INITIAL_READ_REPLICAS ).withNumberOfCoreMembers( INITIAL_CORE_MEMBERS ) );
        cluster.start();
    }

    @Test
    void shouldNotBePossibleToStartDatabaseAfterPanic() throws Exception
    {
        // given: system and default database members are started and visible in topology service
        assertEventuallyNumberOfMembersInTopologies( INITIAL_CORE_MEMBERS, INITIAL_READ_REPLICAS, SYSTEM_DATABASE_NAME );
        assertEventuallyNumberOfMembersInTopologies( INITIAL_CORE_MEMBERS, INITIAL_READ_REPLICAS, DEFAULT_DATABASE_NAME );

        // when: default database panicked on all cores and read replicas
        panicDefaultDatabaseOnAllMembers();
        // then: no members of the default database are in topology service
        assertEventuallyNumberOfMembersInTopologies( 0, 0, DEFAULT_DATABASE_NAME );

        // when: try to stop and start the default database
        attemptToRestartDefaultDatabase();
        // then: no members of the default database are in topology service
        assertNoMembersInDefaultDatabaseTopologies();
        // and: all members of the system database are available in topology service and unaffected by the default database panics
        assertEventuallyNumberOfMembersInTopologies( INITIAL_CORE_MEMBERS, INITIAL_READ_REPLICAS, SYSTEM_DATABASE_NAME );
    }

    @Nested
    @DisplayName( "Should shutdown members on panic" )
    class PanicMembers
    {
        @TestFactory
        Stream<DynamicTest> tests()
        {
            return DynamicTest.stream( contexts( cluster ).iterator(), context -> context.getClass().getSimpleName() + ": " + context.index,
                    this::shouldShutdownOnPanic );
        }

        void shouldShutdownOnPanic( Context context ) throws InterruptedException
        {
            // given
            assertNumberOfMembersInTopology( context.expectedMembersBeforePanic(), DEFAULT_DATABASE_NAME, context );
            var panicService = context.panicService();

            // when
            panicService.panickerFor( context.defaultDbId() ).panic( DatabasePanicReason.Test, new Exception() );

            // then
            // default db shutdown because of the panic
            assertNumberOfMembersInTopology( context.expectedMembersBeforePanic() - 1, DEFAULT_DATABASE_NAME, context );
            // system db remains running and is unaffected by the panic of the default db
            assertNumberOfMembersInTopology( context.initialInstanceCount, SYSTEM_DATABASE_NAME, context );
        }
    }

    private void panicDefaultDatabaseOnAllMembers()
    {
        for ( var member : cluster.allMembers() )
        {
            var defaultDb = member.defaultDatabase();
            var panicService = defaultDb.getDependencyResolver().resolveDependency( PanicService.class );
            var databasePanicker = panicService.panickerFor( defaultDb.databaseId() );
            databasePanicker.panic( DatabasePanicReason.Test, new Exception() );
        }
    }

    private void attemptToRestartDefaultDatabase() throws Exception
    {
        // use a routing driver and a single session so that system database bookmarks are passed between transactions
        try ( var driver = driverFactory.graphDatabaseDriver( cluster );
              var session = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
        {
            session.writeTransaction( tx -> tx.run( "STOP DATABASE " + DEFAULT_DATABASE_NAME ) ).consume();
            session.writeTransaction( tx -> tx.run( "START DATABASE " + DEFAULT_DATABASE_NAME ) ).consume();
            session.writeTransaction( tx -> tx.run( "SHOW DATABASES" ) ).consume();

            for ( var i = 0; i < INITIAL_CORE_MEMBERS + INITIAL_READ_REPLICAS - 1; i++ )
            {
                session.readTransaction( tx -> tx.run( "SHOW DATABASES" ) ).consume();
            }
        }
    }

    private void assertNoMembersInDefaultDatabaseTopologies()
    {
        assertEquals( 0, cluster.numberOfCoreMembersReportedByTopology( DEFAULT_DATABASE_NAME ) );
        assertEquals( 0, cluster.numberOfReadReplicaMembersReportedByTopology( DEFAULT_DATABASE_NAME ) );
    }

    private void assertEventuallyNumberOfMembersInTopologies( int expectedCores, int expectedReadReplicas, String databaseName ) throws Exception
    {
        assertEventually( () -> cluster.numberOfCoreMembersReportedByTopology( databaseName ), equalityCondition( expectedCores ), 3, MINUTES );
        assertEventually( () -> cluster.numberOfReadReplicaMembersReportedByTopology( databaseName ), equalityCondition( expectedReadReplicas ), 3, MINUTES );
    }

    static Stream<Context> contexts( Cluster cluster )
    {
        return Stream.concat( readReplicas( cluster ), cores( cluster ) );
    }

    static Stream<CoreContext> cores( Cluster cluster )
    {
        return IntStream.range( 0, INITIAL_CORE_MEMBERS ).mapToObj( i -> new CoreContext( i, cluster ) );
    }

    static Stream<ReadReplicaContext> readReplicas( Cluster cluster )
    {
        return IntStream.range( 0, INITIAL_READ_REPLICAS ).mapToObj( i -> new ReadReplicaContext( i, cluster ) );
    }

    private static void assertNumberOfMembersInTopology( int expected, String databaseName, Context context )
    {
        assertEventually( () -> context.membersInTopology( databaseName ), equalityCondition( expected ), 3, MINUTES );
    }

    private static class CoreContext extends Context
    {
        CoreContext( int index, Cluster cluster )
        {
            super( index, cluster.coreMembers().size(), cluster );
        }

        @Override
        int membersInTopology( String databaseName )
        {
            return cluster.numberOfCoreMembersReportedByTopology( databaseName );
        }

        @Override
        int expectedMembersBeforePanic()
        {
            return INITIAL_CORE_MEMBERS - index;
        }

        @Override
        ClusterMember member()
        {
            return cluster.getCoreMemberByIndex( index );
        }
    }

    private static class ReadReplicaContext extends Context
    {
        ReadReplicaContext( int index, Cluster cluster )
        {
            super( index, cluster.readReplicas().size(), cluster );
        }

        @Override
        int membersInTopology( String databaseName )
        {
            return cluster.numberOfReadReplicaMembersReportedByTopology( databaseName );
        }

        @Override
        int expectedMembersBeforePanic()
        {
            return INITIAL_READ_REPLICAS - index;
        }

        @Override
        ClusterMember member()
        {
            return cluster.getReadReplicaByIndex( index );
        }
    }

    private abstract static class Context
    {
        final int index;
        final int initialInstanceCount;
        final Cluster cluster;

        Context( int index, int initialInstanceCount, Cluster cluster )
        {
            this.index = index;
            this.initialInstanceCount = initialInstanceCount;
            this.cluster = cluster;
        }

        abstract int membersInTopology( String databaseName );

        abstract int expectedMembersBeforePanic();

        abstract ClusterMember member();

        final PanicService panicService()
        {
            return member().defaultDatabase().getDependencyResolver().resolveDependency( PanicService.class );
        }

        final NamedDatabaseId defaultDbId()
        {
            return member().defaultDatabase().databaseId();
        }
    }
}
