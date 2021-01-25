/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.api.integrationtest;

import com.neo4j.SchemaHelper;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.RelExistenceConstraintDescriptor;

import static org.neo4j.graphdb.RelationshipType.withName;

class RelationshipPropertyExistenceConstraintCreationIT
        extends AbstractConstraintCreationIT<ConstraintDescriptor,RelationTypeSchemaDescriptor>
{
    @Override
    int initializeLabelOrRelType( TokenWrite tokenWrite, String name ) throws KernelException
    {
        return tokenWrite.relationshipTypeGetOrCreateForName( name );
    }

    @Override
    ConstraintDescriptor createConstraint( SchemaWrite writeOps,
            RelationTypeSchemaDescriptor descriptor ) throws Exception
    {
        return writeOps.relationshipPropertyExistenceConstraintCreate( descriptor, null );
    }

    @Override
    void createConstraintInRunningTx( SchemaHelper helper, GraphDatabaseService db, org.neo4j.graphdb.Transaction tx, String type, String property )
    {
        helper.createRelPropertyExistenceConstraint( db, tx, type, property );
    }

    @Override
    RelExistenceConstraintDescriptor newConstraintObject( RelationTypeSchemaDescriptor descriptor )
    {
        return ConstraintDescriptorFactory.existsForSchema( descriptor ).withName( "constraint" );
    }

    @Override
    void dropConstraint( SchemaWrite writeOps, ConstraintDescriptor constraint )
            throws Exception
    {
        writeOps.constraintDrop( constraint );
    }

    @Override
    void createOffendingDataInRunningTx( org.neo4j.graphdb.Transaction tx )
    {
        Node start = tx.createNode();
        Node end = tx.createNode();
        start.createRelationshipTo( end, withName( KEY ) );
    }

    @Override
    void removeOffendingDataInRunningTx( org.neo4j.graphdb.Transaction tx )
    {
        Iterable<Relationship> relationships = tx.getAllRelationships();
        for ( Relationship relationship : relationships )
        {
            relationship.delete();
        }
    }

    @Override
    RelationTypeSchemaDescriptor makeDescriptor( int typeId, int propertyKeyId )
    {
        return SchemaDescriptor.forRelType( typeId, propertyKeyId );
    }
}
