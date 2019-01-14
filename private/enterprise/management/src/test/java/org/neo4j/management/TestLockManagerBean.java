/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.jmx.impl.JmxKernelExtension;
import org.neo4j.kernel.info.LockInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestLockManagerBean
{
    private LockManager lockManager;

    @Rule
    public ImpermanentDbmsRule dbRule = new ImpermanentDbmsRule();
    private GraphDatabaseAPI graphDb;

    @Before
    public void setup()
    {
        graphDb = dbRule.getGraphDatabaseAPI();
        lockManager = graphDb.getDependencyResolver().resolveDependency( JmxKernelExtension.class )
                .getSingleManagementBean( LockManager.class );
    }

    @Test
    public void restingGraphHoldsNoLocks()
    {
        assertEquals( "unexpected lock count", 0, lockManager.getLocks().size() );
    }

    @Test
    public void modifiedNodeImpliesLock()
    {
        Node node = createNode();

        try ( Transaction ignore = graphDb.beginTx() )
        {
            node.setProperty( "key", "value" );

            List<LockInfo> locks = lockManager.getLocks();
            assertEquals( "unexpected lock count", 1, locks.size() );
            LockInfo lock = locks.get( 0 );
            assertNotNull( "null lock", lock );

        }
        List<LockInfo> locks = lockManager.getLocks();
        assertEquals( "unexpected lock count", 0, locks.size() );
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
