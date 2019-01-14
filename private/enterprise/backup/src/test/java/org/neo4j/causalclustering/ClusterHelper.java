/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public class ClusterHelper
{
    public static final Label label = Label.label( "any_label" );
    public static final String PROP_NAME = "name";
    public static final String PROP_RANDOM = "random";

    /**
     * Used by cc
     * @param db
     * @param tx
     */
    public static void createSomeData( GraphDatabaseService db, Transaction tx )
    {
        Node node = db.createNode();
        node.setProperty( PROP_NAME, "Neo" );
        node.setProperty( PROP_RANDOM, Math.random() * 10000 );
        db.createNode().createRelationshipTo( node, RelationshipType.withName( "KNOWS" ) );
        tx.success();
    }

    public static void createIndexes( CoreGraphDatabase db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( PROP_NAME ).on( PROP_RANDOM ).create();
            tx.success();
        }
    }

}
