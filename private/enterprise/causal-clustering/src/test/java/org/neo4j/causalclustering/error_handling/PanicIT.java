/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.error_handling;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.common.EnterpriseCluster;
import org.neo4j.causalclustering.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class PanicIT
{
    private static final int INITIAL_CORE_MEMBERS = 3;
    private static final int INITIAL_READ_REPLICAS = 1;

    @Inject
    private DefaultFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private EnterpriseCluster cluster;

    @BeforeEach
    void startCluster() throws ExecutionException, InterruptedException
    {
        testDirectory.directory();
        cluster = new EnterpriseCluster( testDirectory.directory(), INITIAL_CORE_MEMBERS, INITIAL_READ_REPLICAS, new HazelcastDiscoveryServiceFactory(),
                emptyMap(), emptyMap(), emptyMap(), emptyMap(), Standard.LATEST_NAME, IpFamily.IPV4, false );
        cluster.start();
    }

    @AfterEach
    void shutdownCluster()
    {
        cluster.shutdown();
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
            assertEquals( context.expectedMembersBeforePanic(), context.membersInTopology() );
            PanicService panicService = context.panicService();

            // when
            panicService.panic( null );

            // then
            assertEventually( context::membersInTopology, equalTo( context.expectedMembersBeforePanic() - 1 ), 30, TimeUnit.SECONDS );
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

    private static class CoreContext extends Context
    {

        CoreContext( int instanceNr, Cluster cluster )
        {
            super( instanceNr, cluster );
        }

        @Override
        int membersInTopology()
        {
            return cluster.numberOfCoreMembersReportedByTopology();
        }

        @Override
        int expectedMembersBeforePanic()
        {
            return INITIAL_CORE_MEMBERS - instnaceNr();
        }

        @Override
        PanicService panicService()
        {
            return cluster.getCoreMemberById( instnaceNr() ).database().getDependencyResolver().resolveDependency( PanicService.class );
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
            return cluster.numberOfReadReplicaMembersReportedByTopology();
        }

        @Override
        int expectedMembersBeforePanic()
        {
            return INITIAL_READ_REPLICAS - instnaceNr();
        }

        @Override
        PanicService panicService()
        {
            return cluster.getReadReplicaById( instnaceNr() ).database().getDependencyResolver().resolveDependency( PanicService.class );
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
