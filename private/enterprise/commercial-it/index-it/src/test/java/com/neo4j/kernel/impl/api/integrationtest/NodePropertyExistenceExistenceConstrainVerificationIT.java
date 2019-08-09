/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.api.integrationtest;

import com.neo4j.SchemaHelper;

import java.lang.reflect.Executable;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.impl.newapi.Operations;

import static org.neo4j.graphdb.Label.label;

class NodePropertyExistenceExistenceConstrainVerificationIT extends PropertyExistenceConstraintVerificationIT
{
    @Override
    void createConstraint( SchemaHelper helper, GraphDatabaseService db, String label, String property )
    {
        helper.createNodePropertyExistenceConstraint( db, label, property );
    }

    @Override
    Executable constraintCreationMethod() throws Exception
    {
        return Operations.class.getMethod( "nodePropertyExistenceConstraintCreate", LabelSchemaDescriptor.class, String.class );
    }

    @Override
    void createOffender( GraphDatabaseService db, String key )
    {
        Node node = db.createNode();
        node.addLabel( label( key ) );
    }

    @Override
    Executable offenderCreationMethod() throws Exception
    {
        return Operations.class.getMethod( "nodeAddLabel", long.class, int.class ); // takes schema read lock to enforce constraints
    }
}
