/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.jmx.impl.JmxExtension;
import org.neo4j.kernel.info.LockInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ImpermanentDbmsExtension
class TestLockManagerBean
{
    private LockManager lockManager;

    @Inject
    private GraphDatabaseAPI graphDb;

    @BeforeEach
    void setup()
    {
        lockManager = graphDb.getDependencyResolver().resolveDependency( JmxExtension.class ).getSingleManagementBean( LockManager.class );
    }

    @Test
    void restingGraphHoldsNoLocks()
    {
        assertEquals( 0, lockManager.getLocks().size(), "unexpected lock count" );
    }

    @Test
    void modifiedNodeImpliesLock()
    {
        Node node = createNode();

        try ( Transaction ignore = graphDb.beginTx() )
        {
            node.setProperty( "key", "value" );

            List<LockInfo> locks = lockManager.getLocks();
            assertEquals( 1, locks.size(), "unexpected lock count" );
            LockInfo lock = locks.get( 0 );
            assertNotNull( lock, "null lock" );

        }
        List<LockInfo> locks = lockManager.getLocks();
        assertEquals( 0, locks.size(), "unexpected lock count" );
    }

    private Node createNode()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            Node node = graphDb.createNode();
            tx.success();
            return node;
        }
    }

}
