/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.causalclustering;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClusterFactoryTest
{
    @Nested
    @TestInstance( TestInstance.Lifecycle.PER_METHOD )
    @ClusterExtension
    class MethodToOtherLifecycle
    {
        @Inject
        ClusterFactory clusterFactory;

        int expectedClustersAfterTest;

        @Test
        void oneCluster()
        {
            createClustersAndSetExpected( 1 );
        }

        @Test
        void zeroClusters()
        {
            createClustersAndSetExpected( 0 );
        }

        @Test
        void tenClusters()
        {
            createClustersAndSetExpected( 10 );
        }

        private void createClustersAndSetExpected( int amount )
        {
            expectedClustersAfterTest = amount;
            for ( int i = 0; i < expectedClustersAfterTest; i++ )
            {
                clusterFactory.createCluster( ClusterConfig.clusterConfig() );
            }
        }

        @Nested
        @TestInstance( TestInstance.Lifecycle.PER_CLASS )
        class NestedPerClassUsesParentLife
        {
            @Test
            void aTest()
            {
                createClustersAndSetExpected( 1 );
            }

            @Test
            void anotherTest()
            {
                createClustersAndSetExpected( 1 );
            }
        }

        @Nested
        @TestInstance( TestInstance.Lifecycle.PER_METHOD )
        class NestedPerMethod
        {

            @Test
            void aTest()
            {
                createClustersAndSetExpected( 1 );
            }

            @Test
            void anotherTest()
            {
                createClustersAndSetExpected( 1 );
            }
        }

        @AfterEach
        void assertExpectedClusterSize()
        {
            assertEquals( expectedClustersAfterTest, ((TrackingClusterFactory) clusterFactory).activeClusters() );
        }
    }

    @Nested
    @ClusterExtension
    class ClassToOtherLifecycle
    {
        @Inject
        ClusterFactory clusterFactory;

        int globalActiveClustersInFactory;

        @Test
        void oneCluster()
        {
            createAndAdd( 1 );
        }

        @Test
        void zeroClusters()
        {
            createAndAdd( 0 );
        }

        @Test
        void tenClusters()
        {
            createAndAdd( 10 );
        }

        private void createAndAdd( int amount )
        {
            globalActiveClustersInFactory += amount;
            for ( int i = 0; i < amount; i++ )
            {
                clusterFactory.createCluster( ClusterConfig.clusterConfig() );
            }
        }

        @Nested
        @TestInstance( TestInstance.Lifecycle.PER_CLASS )
        class NestedPerClassAddsToGlobal
        {
            @Test
            void aTest()
            {
                createAndAdd( 1 );
            }

            @Test
            void anotherTest()
            {
                createAndAdd( 1 );
            }
        }

        @Nested
        @TestInstance( TestInstance.Lifecycle.PER_METHOD )
        class NestedMethodAddsToGlobal
        {
            @Test
            void createCluster()
            {
                createAndAdd( 1 );
            }
        }

        @BeforeEach
        void assertExpectedClusterSize()
        {
            assertEquals( globalActiveClustersInFactory, ((TrackingClusterFactory) clusterFactory).activeClusters() );
        }
    }

    @Nested
    @ClusterExtension
    class CheckFactoryIsSameInNested
    {
        @Inject
        ClusterFactory clusterFactory;

        @Nested
        class NestedInjected
        {
            @Inject
            ClusterFactory nestedClusterFactory;

            @Test
            void shouldBeSameFactory()
            {
                assertEquals( clusterFactory, nestedClusterFactory );
            }
        }
    }
}
