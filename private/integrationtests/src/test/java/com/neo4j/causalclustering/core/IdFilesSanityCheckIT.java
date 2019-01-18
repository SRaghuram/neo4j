/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.test.causalclustering.ClusterRule;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;

import com.neo4j.causalclustering.common.Cluster;
import org.neo4j.graphdb.Node;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.store.id.IdContainer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class IdFilesSanityCheckIT
{
    @Rule
    public final ClusterRule clusterRule =
            new ClusterRule()
                    .withNumberOfCoreMembers( 3 )
                    .withNumberOfReadReplicas( 0 )
                    .withTimeout( 1000, SECONDS );

    private Cluster<?> cluster;
    private FileSystemAbstraction fs = clusterRule.testDirectory().getFileSystem();

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void shouldRemoveIdFilesFromUnboundInstance() throws Exception
    {
        // given
        int nodeCount = 100;

        ArrayList<Node> nodes = new ArrayList<>( nodeCount );

        cluster.coreTx( ( db, tx ) ->
        {
            for ( int i = 0; i < nodeCount; i++ )
            {
                nodes.add( db.createNode() );
                tx.success();
            }
        } );

        CoreClusterMember leader = cluster.coreTx( ( db, tx ) ->
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
        DatabaseLayout databaseLayout = DatabaseLayout.of( leader.databaseDirectory() );
        long freeIdCount = getFreeIdCount( databaseLayout.idNodeStore() );
        assertEquals( 0, freeIdCount );
    }

    private long getFreeIdCount( File idFile )
    {
        IdContainer idContainer = new IdContainer( fs, idFile, 1024, true );
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
