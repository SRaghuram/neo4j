/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel;

import com.neo4j.kernel.impl.pagecache.monitor.PageCacheWarmerMonitor;
import com.neo4j.kernel.impl.pagecache.monitor.PageCacheWarmerMonitorAdapter;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.util.concurrent.BinaryLatch;

class PageCacheWarmupTestSupport
{
    static void createTestData( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Label label = Label.label( "Label" );
            RelationshipType relationshipType = RelationshipType.withName( "REL" );
            long[] largeValue = new long[1024];
            for ( int i = 0; i < 1000; i++ )
            {
                Node node = db.createNode( label );
                node.setProperty( "Niels", "Borh" );
                node.setProperty( "Albert", largeValue );
                for ( int j = 0; j < 30; j++ )
                {
                    Relationship rel = node.createRelationshipTo( node, relationshipType );
                    rel.setProperty( "Max", "Planck" );
                }
            }
            tx.success();
        }
    }

    static long waitForCacheProfile( Monitors monitors )
    {
        AtomicLong pageCount = new AtomicLong();
        BinaryLatch profileLatch = new BinaryLatch();
        PageCacheWarmerMonitor listener = new AwaitProfileMonitor( pageCount, profileLatch );
        monitors.addMonitorListener( listener );
        profileLatch.await();
        monitors.removeMonitorListener( listener );
        return pageCount.get();
    }

    static BinaryLatch pauseProfile( Monitors monitors )
    {
        return new PauseProfileMonitor( monitors );
    }

    private static class AwaitProfileMonitor extends PageCacheWarmerMonitorAdapter
    {
        private final AtomicLong pageCount;
        private final BinaryLatch profileLatch;

        AwaitProfileMonitor( AtomicLong pageCount, BinaryLatch profileLatch )
        {
            this.pageCount = pageCount;
            this.profileLatch = profileLatch;
        }

        @Override
        public void profileCompleted( long pagesInMemory )
        {
            pageCount.set( pagesInMemory );
            profileLatch.release();
        }
    }

    private static class PauseProfileMonitor extends BinaryLatch implements PageCacheWarmerMonitor
    {
        private final Monitors monitors;

        PauseProfileMonitor( Monitors monitors )
        {
            this.monitors = monitors;
            monitors.addMonitorListener( this );
        }

        @Override
        public void warmupStarted()
        {
            //nothing
        }

        @Override
        public void warmupCompleted( long pagesLoaded )
        {
            //nothing
        }

        @Override
        public void profileCompleted( long pagesInMemory )
        {
            await();
            monitors.removeMonitorListener( this );
        }
    }
}
