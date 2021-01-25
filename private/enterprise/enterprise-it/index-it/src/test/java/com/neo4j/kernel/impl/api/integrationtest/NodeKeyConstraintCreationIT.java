/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.api.integrationtest;

import com.neo4j.SchemaHelper;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;

class NodeKeyConstraintCreationIT extends AbstractConstraintCreationIT<ConstraintDescriptor,LabelSchemaDescriptor>
{
    @Override
    int initializeLabelOrRelType( TokenWrite tokenWrite, String name ) throws KernelException
    {
        return tokenWrite.labelGetOrCreateForName( name );
    }

    @Override
    ConstraintDescriptor createConstraint( SchemaWrite writeOps, LabelSchemaDescriptor schema )
            throws Exception
    {
        return writeOps.nodeKeyConstraintCreate( IndexPrototype.uniqueForSchema( schema ) );
    }

    @Override
    void createConstraintInRunningTx( SchemaHelper helper, GraphDatabaseService db, org.neo4j.graphdb.Transaction tx, String type, String property )
    {
        helper.createNodeKeyConstraint( db, tx, type, property );
    }

    @Override
    IndexBackedConstraintDescriptor newConstraintObject( LabelSchemaDescriptor descriptor )
    {
        return ConstraintDescriptorFactory.nodeKeyForSchema( descriptor ).withName( "constraint" );
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
        tx.createNode( Label.label( KEY ) );
    }

    @Override
    void removeOffendingDataInRunningTx( org.neo4j.graphdb.Transaction tx )
    {
        try ( ResourceIterator<Node> nodes = tx.findNodes( Label.label( KEY ) ) )
        {
            while ( nodes.hasNext() )
            {
                nodes.next().delete();
            }
        }
    }

    @Override
    LabelSchemaDescriptor makeDescriptor( int typeId, int propertyKeyId )
    {
        return SchemaDescriptor.forLabel( typeId, propertyKeyId );
    }
}
