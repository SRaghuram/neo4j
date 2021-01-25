/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.api.integrationtest;

import com.neo4j.SchemaHelper;

import java.lang.reflect.Executable;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.impl.newapi.Operations;

import static org.neo4j.graphdb.RelationshipType.withName;

class RelationshipPropertyExistenceExistenceConstrainVerificationIT extends PropertyExistenceConstraintVerificationIT
{
    @Override
    public void createConstraint( SchemaHelper helper, GraphDatabaseService db, Transaction tx, String relType, String property )
    {
        helper.createRelPropertyExistenceConstraint( db, tx, relType, property );
    }

    @Override
    public Executable constraintCreationMethod() throws Exception
    {
        return Operations.class.getMethod( "relationshipPropertyExistenceConstraintCreate", RelationTypeSchemaDescriptor.class, String.class );
    }

    @Override
    public void createOffender( Transaction tx, String key )
    {
        Node start = tx.createNode();
        Node end = tx.createNode();
        start.createRelationshipTo( end, withName( key ) );
    }

    @Override
    public Executable offenderCreationMethod() throws Exception
    {
        return Operations.class.getMethod( "relationshipCreate", long.class, int.class, long.class ); // takes schema read lock to enforce constraints
    }
}
