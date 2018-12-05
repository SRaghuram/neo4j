/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.api.integrationtest;

import com.neo4j.SchemaHelper;
import org.junit.Test;

import java.util.Iterator;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchConstraintException;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constraints.NodeExistenceConstraintDescriptor;
import org.neo4j.storageengine.api.schema.ConstraintDescriptor;
import org.neo4j.storageengine.api.schema.LabelSchemaDescriptor;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterators.single;

public class NodePropertyExistenceConstraintCreationIT
        extends AbstractConstraintCreationIT<ConstraintDescriptor,LabelSchemaDescriptor>
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
        return writeOps.nodePropertyExistenceConstraintCreate( descriptor );
    }

    @Override
    void createConstraintInRunningTx( GraphDatabaseService db, String label, String property )
    {
        SchemaHelper.createNodePropertyExistenceConstraint( db, label, property );
    }

    @Override
    NodeExistenceConstraintDescriptor newConstraintObject( LabelSchemaDescriptor descriptor )
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
        db.createNode( label( KEY ) );
    }

    @Override
    void removeOffendingDataInRunningTx( GraphDatabaseService db )
    {
        try ( ResourceIterator<Node> nodes = db.findNodes( label( KEY ) ) )
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
        return SchemaDescriptorFactory.forLabel( typeId, propertyKeyId );
    }

    @Test
    public void shouldNotDropPropertyExistenceConstraintThatDoesNotExistWhenThereIsAUniquePropertyConstraint()
            throws Exception
    {
        // given
        ConstraintDescriptor constraint;
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            constraint = statement.uniquePropertyConstraintCreate( descriptor );
            commit();
        }

        // when
        try
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            statement.constraintDrop( ConstraintDescriptorFactory.existsForSchema( constraint.schema() ) );

            fail( "expected exception" );
        }
        // then
        catch ( DropConstraintFailureException e )
        {
            assertThat( e.getCause(), instanceOf( NoSuchConstraintException.class ) );
        }
        finally
        {
            rollback();
        }

        // then
        {
            Transaction transaction = newTransaction();

            Iterator<ConstraintDescriptor> constraints = transaction.schemaRead().constraintsGetForSchema( descriptor );

            assertEquals( constraint, single( constraints ) );
            commit();
        }
    }
}
