/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import org.neo4j.graphdb.Node;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.Inject;

import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ClusterExtension
class IdFilesSanityCheckIT
{
    @Inject
    private static ClusterFactory clusterFactory;

    private static Cluster cluster;

    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    @BeforeAll
    static void beforeAll() throws Exception
    {
        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @AfterEach
    void afterEach() throws Exception
    {
        fs.close();
    }

    @Test
    public void unbindingStateShouldNotAffectIdGeneratorState() throws Exception
    {
        // Creates a few nodes on the leader, deletes them, unbinds the leader. The free id count should remain correct
        // given
        var nodeCount = 102;

        var nodes = new ArrayList<Node>( nodeCount );

        cluster.coreTx( ( db, tx ) ->
        {
            for ( var i = 0; i < nodeCount; i++ )
            {
                nodes.add( tx.createNode() );
            }
            tx.commit();
        } );

        var leader = cluster.coreTx( ( db, tx ) ->
        {
            nodes.forEach( n -> tx.getNodeById( n.getId() ).delete() );
            tx.commit();
        } );

        // when
        leader.shutdown();
        leader.unbind( fs );
        leader.start();

        IdGenerator nodeIds =
                leader.database( "neo4j" ).getDependencyResolver()
                        .resolveDependency( IdGeneratorFactory.class ).get( IdType.NODE );
        long idsUnused = nodeIds.getDefragCount();
        leader.shutdown(); // test is over, we can shut this down

        // then
        assertEquals( nodeCount, idsUnused );
    }
}
