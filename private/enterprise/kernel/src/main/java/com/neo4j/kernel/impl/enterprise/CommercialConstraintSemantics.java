/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.constraints.NodeKeyConstraintDescriptor;
import org.neo4j.kernel.api.exceptions.schema.NodePropertyExistenceException;
import org.neo4j.kernel.api.exceptions.schema.RelationshipPropertyExistenceException;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

import static com.neo4j.kernel.impl.enterprise.PropertyExistenceEnforcer.getOrCreatePropertyExistenceEnforcerFrom;
import static org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException.Phase.VERIFICATION;

@ServiceProvider
public class CommercialConstraintSemantics extends StandardConstraintSemantics
{
    public CommercialConstraintSemantics()
    {
        super( 2 );
    }

    @Override
    public String getName()
    {
        return "commercialConstraints";
    }
    @Override
    protected ConstraintDescriptor readNonStandardConstraint( ConstraintDescriptor constraint, String errorMessage )
    {
        if ( !constraint.enforcesPropertyExistence() )
        {
            throw new IllegalStateException( "Unsupported constraint type: " + constraint );
        }
        return constraint;
    }

    @Override
    public ConstraintDescriptor createNodeKeyConstraintRule(
            long ruleId, NodeKeyConstraintDescriptor descriptor, long indexId )
    {
        return accessor.createNodeKeyConstraintRule( ruleId, descriptor, indexId );
    }

    @Override
    public ConstraintDescriptor createExistenceConstraint( long ruleId, ConstraintDescriptor descriptor )
    {
        return accessor.createExistenceConstraint( ruleId, descriptor );
    }

    @Override
    public void validateNodePropertyExistenceConstraint( NodeLabelIndexCursor allNodes, NodeCursor nodeCursor,
            PropertyCursor propertyCursor, LabelSchemaDescriptor descriptor )
            throws CreateConstraintFailureException
    {
        while ( allNodes.next() )
        {
            allNodes.node( nodeCursor );
            while ( nodeCursor.next() )
            {
                for ( int propertyKey : descriptor.getPropertyIds() )
                {
                    nodeCursor.properties( propertyCursor );
                    if ( !hasProperty( propertyCursor, propertyKey ) )
                    {
                        throw createConstraintFailure(
                                new NodePropertyExistenceException( descriptor, VERIFICATION,
                                        nodeCursor.nodeReference() ) );
                    }
                }
            }
        }
    }

    @Override
    public void validateNodeKeyConstraint( NodeLabelIndexCursor allNodes, NodeCursor nodeCursor,
            PropertyCursor propertyCursor, LabelSchemaDescriptor descriptor ) throws CreateConstraintFailureException
    {
        validateNodePropertyExistenceConstraint( allNodes, nodeCursor, propertyCursor, descriptor );
    }

    private boolean hasProperty( PropertyCursor propertyCursor, int property )
    {
        while ( propertyCursor.next() )
        {
            if ( propertyCursor.propertyKey() == property )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public void validateRelationshipPropertyExistenceConstraint( RelationshipScanCursor relationshipCursor,
            PropertyCursor propertyCursor, RelationTypeSchemaDescriptor descriptor )
            throws CreateConstraintFailureException
    {
        while ( relationshipCursor.next() )
        {
            relationshipCursor.properties( propertyCursor );

            for ( int propertyKey : descriptor.getPropertyIds() )
            {
                if ( relationshipCursor.type() == descriptor.getRelTypeId() &&
                     !hasProperty( propertyCursor, propertyKey ) )
                {
                    throw createConstraintFailure(
                            new RelationshipPropertyExistenceException( descriptor, VERIFICATION,
                                    relationshipCursor.relationshipReference() ) );
                }
            }
        }
    }

    private CreateConstraintFailureException createConstraintFailure( ConstraintValidationException it )
    {
        return new CreateConstraintFailureException( it.constraint(), it );
    }

    @Override
    public TxStateVisitor decorateTxStateVisitor( StorageReader storageReader,
            Read read, CursorFactory cursorFactory, ReadableTransactionState txState, TxStateVisitor visitor )
    {
        if ( !txState.hasDataChanges() )
        {
            // If there are no data changes, there is no need to enforce constraints. Since there is no need to
            // enforce constraints, there is no need to build up the state required to be able to enforce constraints.
            // In fact, it might even be counter productive to build up that state, since if there are no data changes
            // there would be schema changes instead, and in that case we would throw away the schema-dependant state
            // we just built when the schema changing transaction commits.
            return visitor;
        }
        return getOrCreatePropertyExistenceEnforcerFrom( storageReader )
                .decorate( visitor, read, cursorFactory );
    }
}
