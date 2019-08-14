/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.test.extension.Inject;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

@Disabled
@ClusterExtension
@TestInstance( TestInstance.Lifecycle.PER_METHOD )
class PanicIT
{
    private static final int INITIAL_CORE_MEMBERS = 3;
    private static final int INITIAL_READ_REPLICAS = 1;

    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeEach
    void startCluster() throws ExecutionException, InterruptedException
    {
        cluster = clusterFactory.createCluster(
                ClusterConfig.clusterConfig().withNumberOfReadReplicas( INITIAL_READ_REPLICAS ).withNumberOfCoreMembers( INITIAL_CORE_MEMBERS ) );
        this.cluster.start();
    }

    @Nested
    @DisplayName( "Should shutdown members on panic" )
    class PanicMembers
    {
        @TestFactory
        Stream<DynamicTest> tests()
        {
            return DynamicTest.stream( contexts( cluster ).iterator(), context -> context.getClass().getSimpleName() + ": " + context.instanceNr,
                    this::shouldShutdownOnPanic );
        }

        void shouldShutdownOnPanic( Context context ) throws InterruptedException
        {
            // given
            assertNumberOfMembersInTopology( context.expectedMembersBeforePanic(), context );
            PanicService panicService = context.panicService();

            // when
            panicService.panic( null );

            // then
            assertNumberOfMembersInTopology( context.expectedMembersBeforePanic() - 1, context );
        }
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

    private static void assertNumberOfMembersInTopology( int expected, Context context ) throws InterruptedException
    {
        assertEventually( context::membersInTopology, equalTo( expected ), 3, MINUTES );
    }

    private static class CoreContext extends Context
    {

        CoreContext( int instanceNr, Cluster cluster )
        {
            super( instanceNr, cluster );
        }

        @Override
        int membersInTopology()
        {
            return cluster.numberOfCoreMembersReportedByTopology( DEFAULT_DATABASE_NAME );
        }

        @Override
        int expectedMembersBeforePanic()
        {
            return INITIAL_CORE_MEMBERS - instnaceNr();
        }

        @Override
        PanicService panicService()
        {
            return cluster.getCoreMemberById( instnaceNr() ).defaultDatabase().getDependencyResolver().resolveDependency( PanicService.class );
        }
    }

    private static class ReadReplicaContext extends Context
    {

        ReadReplicaContext( int instanceNr, Cluster cluster )
        {
            super( instanceNr, cluster );
        }

        @Override
        int membersInTopology()
        {
            return cluster.numberOfReadReplicaMembersReportedByTopology( DEFAULT_DATABASE_NAME );
        }

        @Override
        int expectedMembersBeforePanic()
        {
            return INITIAL_READ_REPLICAS - instnaceNr();
        }

        @Override
        PanicService panicService()
        {
            return cluster.getReadReplicaById( instnaceNr() ).defaultDatabase().getDependencyResolver().resolveDependency( PanicService.class );
        }
    }

    private abstract static class Context
    {
        private final int instanceNr;
        protected final Cluster cluster;

        Context( int instanceNr, Cluster cluster )
        {
            this.instanceNr = instanceNr;
            this.cluster = cluster;
        }

        abstract int membersInTopology();

        abstract int expectedMembersBeforePanic();

        abstract PanicService panicService();

        int instnaceNr()
        {
            return instanceNr;
        }
    }
}
