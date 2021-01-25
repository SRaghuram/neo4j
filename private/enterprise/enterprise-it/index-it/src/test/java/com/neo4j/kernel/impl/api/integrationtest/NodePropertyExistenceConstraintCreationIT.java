/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.api.integrationtest;

import com.neo4j.SchemaHelper;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.NodeExistenceConstraintDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchConstraintException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterators.single;

class NodePropertyExistenceConstraintCreationIT extends AbstractConstraintCreationIT<ConstraintDescriptor,LabelSchemaDescriptor>
{
    @Override
    int initializeLabelOrRelType( TokenWrite tokenWrite, String name ) throws KernelException
    {
        return tokenWrite.labelGetOrCreateForName( name );
    }

    @Override
    ConstraintDescriptor createConstraint( SchemaWrite writeOps, LabelSchemaDescriptor descriptor )
            throws Exception
    {
        return writeOps.nodePropertyExistenceConstraintCreate( descriptor, null );
    }

    @Override
    void createConstraintInRunningTx( SchemaHelper helper, GraphDatabaseService db, org.neo4j.graphdb.Transaction tx, String label, String property )
    {
        helper.createNodePropertyExistenceConstraint( db, tx, label, property );
    }

    @Override
    NodeExistenceConstraintDescriptor newConstraintObject( LabelSchemaDescriptor descriptor )
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
        tx.createNode( label( KEY ) );
    }

    @Override
    void removeOffendingDataInRunningTx( org.neo4j.graphdb.Transaction tx )
    {
        try ( ResourceIterator<Node> nodes = tx.findNodes( label( KEY ) ) )
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

    @Test
    void shouldNotDropPropertyExistenceConstraintThatDoesNotExistWhenThereIsAUniquePropertyConstraint()
            throws Exception
    {
        // given
        ConstraintDescriptor constraint;
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            IndexPrototype prototype = IndexPrototype.uniqueForSchema( schema ).withName( "constraint name" );
            constraint = statement.uniquePropertyConstraintCreate( prototype );
            commit();
        }

        // when
        var e = assertThrows( DropConstraintFailureException.class, () ->
        {
            try
            {
                SchemaWrite statement = schemaWriteInNewTransaction();
                statement.constraintDrop( ConstraintDescriptorFactory.existsForSchema( constraint.schema() ).withName( "other constraint" ) );
            }
            finally
            {
                rollback();
            }
        } );
        assertThat( e.getCause() ).isInstanceOf( NoSuchConstraintException.class );

        // then
        {
            KernelTransaction transaction = newTransaction();

            Iterator<ConstraintDescriptor> constraints = transaction.schemaRead().constraintsGetForSchema( schema );

            assertEquals( constraint, single( constraints ) );
            commit();
        }
    }
}
