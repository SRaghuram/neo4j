/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.causalclustering;

import com.neo4j.causalclustering.common.Cluster;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import org.neo4j.graphdb.Label;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClusterFactoryIT
{
    @Nested
    @TestInstance( TestInstance.Lifecycle.PER_METHOD )
    class PerMethod extends BaseClusterFactoryTest
    {
        @BeforeEach
        void createAndStartClusterWithNode() throws Exception
        {
            cluster = createAndStartCluster();
        }
    }

    @Nested
    class PerClass extends BaseClusterFactoryTest
    {
        @BeforeAll
        void createAndStartClusterWithNode() throws Exception
        {
            this.cluster = createAndStartCluster();
        }
    }

    @ClusterExtension
    class BaseClusterFactoryTest
    {
        @Inject
        ClusterFactory clusterFactory;

        Cluster cluster;

        Label uniqueLabel = Label.label( "foo" );

        @Test
        void checkClusterIsRunningAndEqualToDefault() throws Exception
        {
            isRunningAndContainData( cluster );
        }

        @Test
        void canCreateAndStartAnotherCluster() throws Exception
        {
            isRunningAndContainData( cluster );
            Cluster cluster = clusterFactory.createCluster( ClusterConfig.clusterConfig() );
            cluster.start();
            isRunningAndNotContainingData( cluster );
            cluster.shutdown();
        }

        @Nested
        class NestedClassIsFine
        {
            @Inject
            ClusterFactory factory;

            @Test
            void checkClusterIsRunning() throws Exception
            {
                isRunningAndContainData( cluster );
            }
        }

        private void isRunningAndNotContainingData( Cluster cluster ) throws Exception
        {
            cluster.awaitLeader();
            cluster.coreTx( ( coreGraphDatabase, transaction ) -> assertEquals( 0,
                    coreGraphDatabase.getAllNodes().stream().filter( node -> node.hasLabel( uniqueLabel ) ).count() ) );
        }

        private void isRunningAndContainData( Cluster cluster ) throws Exception
        {
            cluster.awaitLeader();
            cluster.coreTx( ( coreGraphDatabase, transaction ) -> assertEquals( 1,
                    coreGraphDatabase.getAllNodes().stream().filter( node -> node.hasLabel( uniqueLabel ) ).count() ) );
        }

        Cluster createAndStartCluster() throws Exception
        {
            cluster = clusterFactory.createCluster( ClusterConfig.clusterConfig() );
            cluster.start();
            cluster.coreTx( ( coreGraphDatabase, transaction ) ->
            {
                coreGraphDatabase.createNode( uniqueLabel );
                transaction.success();
            } );
            return cluster;
        }
    }
}
