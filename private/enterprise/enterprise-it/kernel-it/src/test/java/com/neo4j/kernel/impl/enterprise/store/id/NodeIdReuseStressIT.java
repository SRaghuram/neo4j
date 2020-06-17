/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.store.id;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;

import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@EnterpriseDbmsExtension
class NodeIdReuseStressIT
{
    private static final int CONTESTANTS_COUNT = 12;
    private static final int INITIAL_NODE_COUNT = 10_000;
    private static final int OPERATIONS_COUNT = 10_000;
    private static final int THRESHOLD = INITIAL_NODE_COUNT / 10;

    @Inject
    private GraphDatabaseService db;

    @BeforeEach
    void verifyParams()
    {
        assertThat( CONTESTANTS_COUNT ).isGreaterThan( 0 );
        assertThat( CONTESTANTS_COUNT % 2 ).isEqualTo( 0 );
        assertThat( INITIAL_NODE_COUNT ).isGreaterThan( 0 );
        assertThat( OPERATIONS_COUNT ).isGreaterThan( 1_000 );
        assertThat( THRESHOLD ).isGreaterThan( 0 );
        assertThat( THRESHOLD ).isLessThan( INITIAL_NODE_COUNT );
    }

    @Test
    void nodeIdsReused() throws Throwable
    {
        // given
        BitSet initialIds = createInitialNodes( db );
        AtomicInteger numReusedIds = new AtomicInteger();

        // when
        Race race = new Race().withEndCondition( () -> numReusedIds.get() >= THRESHOLD );
        for ( int i = 0; i < CONTESTANTS_COUNT; i++ )
        {
            if ( i % 2 == 0 )
            {
                race.addContestant( new NodeCreator( db, initialIds, numReusedIds ), OPERATIONS_COUNT );
            }
            else
            {
                race.addContestant( new NodeRemover( db ), OPERATIONS_COUNT );
            }
        }
        race.go();

        // then
        assertThat( numReusedIds.get() ).isGreaterThanOrEqualTo( THRESHOLD );
    }

    private static BitSet createInitialNodes( GraphDatabaseService db )
    {
        BitSet ids = new BitSet();
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < INITIAL_NODE_COUNT; i++ )
            {
                ids.set( toIntExact( tx.createNode().getId() ) );
            }
            tx.commit();
        }
        return ids;
    }

    private static long highestNodeId( GraphDatabaseService db )
    {
        DependencyResolver resolver = dependencyResolver( db );
        NeoStores neoStores = resolver.resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        NodeStore nodeStore = neoStores.getNodeStore();
        return nodeStore.getHighestPossibleIdInUse( NULL );
    }

    private static DependencyResolver dependencyResolver( GraphDatabaseService db )
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver();
    }

    private static class NodeCreator implements Runnable
    {
        final GraphDatabaseService db;
        private final BitSet initialIds;
        private final AtomicInteger numReusedIds;

        NodeCreator( GraphDatabaseService db, BitSet initialIds, AtomicInteger numReusedIds )
        {
            this.db = db;
            this.initialIds = initialIds;
            this.numReusedIds = numReusedIds;
        }

        @Override
        public void run()
        {
            Node createdNode;
            try ( Transaction tx = db.beginTx() )
            {
                createdNode = tx.createNode();
                tx.commit();
            }
            if ( initialIds.get( toIntExact( createdNode.getId() ) ) )
            {
                numReusedIds.incrementAndGet();
            }
        }
    }

    private static class NodeRemover implements Runnable
    {
        final GraphDatabaseService db;

        NodeRemover( GraphDatabaseService db )
        {
            this.db = db;
        }

        @Override
        public void run()
        {
            long highestId = highestNodeId( db );
            if ( highestId > 0 )
            {
                long id = ThreadLocalRandom.current().nextLong( highestId );

                try ( Transaction tx = db.beginTx() )
                {
                    tx.getNodeById( id ).delete();
                    tx.commit();
                }
                catch ( NotFoundException ignore )
                {
                    // same node was removed concurrently
                }
            }
        }
    }
}
