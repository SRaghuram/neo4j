/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.neo4j.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constraints.RelExistenceConstraintDescriptor;
import org.neo4j.storageengine.api.schema.ConstraintDescriptor;

import static org.neo4j.graphdb.RelationshipType.withName;

public class RelationshipPropertyExistenceConstraintCreationIT
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
        return writeOps.relationshipPropertyExistenceConstraintCreate( descriptor );
    }

    @Override
    void createConstraintInRunningTx( GraphDatabaseService db, String type, String property )
    {
        SchemaHelper.createRelPropertyExistenceConstraint( db, type, property );
    }

    @Override
    RelExistenceConstraintDescriptor newConstraintObject( RelationTypeSchemaDescriptor descriptor )
    {
        return ConstraintDescriptorFactory.existsForSchema( descriptor );
    }

    @Override
    void dropConstraint( SchemaWrite writeOps, ConstraintDescriptor constraint )
            throws Exception
    {
        writeOps.constraintDrop( constraint );
    }

    @Override
    void createOffendingDataInRunningTx( GraphDatabaseService db )
    {
        Node start = db.createNode();
        Node end = db.createNode();
        start.createRelationshipTo( end, withName( KEY ) );
    }

    @Override
    void removeOffendingDataInRunningTx( GraphDatabaseService db )
    {
        Iterable<Relationship> relationships = db.getAllRelationships();
        for ( Relationship relationship : relationships )
        {
            relationship.delete();
        }
    }

    @Override
    RelationTypeSchemaDescriptor makeDescriptor( int typeId, int propertyKeyId )
    {
        return SchemaDescriptorFactory.forRelType( typeId, propertyKeyId );
    }
}
