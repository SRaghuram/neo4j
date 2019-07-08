/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.io.File;
import java.util.ArrayList;

import org.neo4j.graphdb.Node;
import org.neo4j.internal.id.IdContainer;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.Inject;

import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.concurrent.TimeUnit.SECONDS;
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
                .withNumberOfReadReplicas( 0 )
                .withTimeout( 1000, SECONDS );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @AfterEach
    void afterEach() throws Exception
    {
        fs.close();
    }

    @Test
    void shouldRemoveIdFilesFromUnboundInstance() throws Exception
    {
        // given
        var nodeCount = 100;

        var nodes = new ArrayList<Node>( nodeCount );

        cluster.coreTx( ( db, tx ) ->
        {
            for ( var i = 0; i < nodeCount; i++ )
            {
                nodes.add( db.createNode() );
                tx.success();
            }
        } );

        var leader = cluster.coreTx( ( db, tx ) ->
        {
            nodes.forEach( Node::delete );
            tx.success();
        } );

        // when
        leader.shutdown();
        fs.deleteRecursively( leader.clusterStateDirectory() ); // "unbind"
        leader.start();

        // then
        leader.shutdown(); // we need to shutdown to access the ID-files "raw"
        var freeIdCount = getFreeIdCount( leader.databaseLayout().idNodeStore() );
        assertEquals( 0, freeIdCount );
    }

    private long getFreeIdCount( File idFile )
    {
        var idContainer = new IdContainer( fs, idFile, 1024, true );
        idContainer.init();
        try
        {
            return idContainer.getFreeIdCount();
        }
        finally
        {
            idContainer.close( 0 );
        }
    }
}
