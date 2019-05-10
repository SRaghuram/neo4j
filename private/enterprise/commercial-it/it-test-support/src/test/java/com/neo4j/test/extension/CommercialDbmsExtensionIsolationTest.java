/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.extension;

import org.junit.jupiter.api.RepeatedTest;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertFalse;

@CommercialDbmsExtension
class CommercialDbmsExtensionIsolationTest
{
    @Inject
    private GraphDatabaseService db;

    @RepeatedTest( 5 )
    void shouldBeIsolated()
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertFalse( db.getAllNodes().iterator().hasNext() );
            db.createNode();
            tx.success();
        }
    }
}
