/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.store.id;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@EnterpriseDbmsExtension
class NodeIdReuseStressIT
{
    private static final int CONTESTANTS_COUNT = 12;
    private static final int INITIAL_NODE_COUNT = 10_000;
    private static final int OPERATIONS_COUNT = 10_000;

    @Inject
    private GraphDatabaseService db;

    @BeforeEach
    void verifyParams()
    {
        assertThat( CONTESTANTS_COUNT ).isGreaterThan( 0 );
        assertThat( CONTESTANTS_COUNT % 2 ).isEqualTo( 0 );
        assertThat( INITIAL_NODE_COUNT ).isGreaterThan( 0 );
        assertThat( OPERATIONS_COUNT ).isGreaterThan( 1_000 );
    }

    @Test
    void nodeIdsReused() throws Throwable
    {
        createInitialNodes( db );
        long initialHighestNodeId = highestNodeId( db );

        Race race = new Race();

        for ( int i = 0; i < CONTESTANTS_COUNT; i++ )
        {
            if ( i % 2 == 0 )
            {
                race.addContestant( new NodeCreator( db ) );
            }
            else
            {
                race.addContestant( new NodeRemover( db ) );
            }
        }

        race.go();

        int writeContestants = CONTESTANTS_COUNT / 2;
        int createdNodes = writeContestants * OPERATIONS_COUNT;
        long highestNodeIdWithoutReuse = initialHighestNodeId + createdNodes;

        long currentHighestNodeId = highestNodeId( db );

        assertThat( currentHighestNodeId ).isLessThan( highestNodeIdWithoutReuse );
    }

    private static void createInitialNodes( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < INITIAL_NODE_COUNT; i++ )
            {
                tx.createNode();
            }
            tx.commit();
        }
    }

    private static long highestNodeId( GraphDatabaseService db )
    {
        DependencyResolver resolver = dependencyResolver( db );
        NeoStores neoStores = resolver.resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        NodeStore nodeStore = neoStores.getNodeStore();
        return nodeStore.getHighestPossibleIdInUse();
    }

    private static void maybeRunIdMaintenance( GraphDatabaseService db, int iteration )
    {
        if ( iteration % 100 == 0 && ThreadLocalRandom.current().nextBoolean() )
        {
            DependencyResolver resolver = dependencyResolver( db );
            IdController idController = resolver.resolveDependency( IdController.class );
            if ( idController != null )
            {
                idController.maintenance();
            }
            else
            {
                System.out.println( "Id controller is null. Dumping resolver content." );
                System.out.println( "Resolver: " + ReflectionToStringBuilder.toString( resolver ) );
                throw new IllegalStateException( "Id controller not found" );
            }
        }
    }

    private static DependencyResolver dependencyResolver( GraphDatabaseService db )
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver();
    }

    private static class NodeCreator implements Runnable
    {
        final GraphDatabaseService db;

        NodeCreator( GraphDatabaseService db )
        {
            this.db = db;
        }

        @Override
        public void run()
        {
            for ( int i = 0; i < OPERATIONS_COUNT; i++ )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    tx.createNode();
                    tx.commit();
                }

                maybeRunIdMaintenance( db, i );
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
            for ( int i = 0; i < OPERATIONS_COUNT; i++ )
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

                maybeRunIdMaintenance( db, i );
            }
        }
    }
}
