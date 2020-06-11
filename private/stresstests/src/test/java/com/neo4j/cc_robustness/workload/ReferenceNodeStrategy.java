/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload;

import com.neo4j.cc_robustness.ReferenceNode;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public enum ReferenceNodeStrategy
{
    same
            {
                @Override
                public Node find( GraphDatabaseService db, Transaction tx )
                {
                    return ReferenceNode.findReferenceNode( tx );
                }
            },
    mixed
            {
                @Override
                public Node find( GraphDatabaseService db, Transaction tx )
                {
                    return System.currentTimeMillis() % 5 == 0 ? ReferenceNode.findReferenceNode( tx ) : tx.createNode();
                }
            },
    isolated
            {
                @Override
                public Node find( GraphDatabaseService db, Transaction tx )
                {
                    return tx.createNode();
                }
            };

    public abstract Node find( GraphDatabaseService db, Transaction tx );
}
