/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.Optional;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.impl.coreapi.schema.NodeKeyConstraintDefinition;
import org.neo4j.kernel.impl.coreapi.schema.NodePropertyExistenceConstraintDefinition;
import org.neo4j.kernel.impl.coreapi.schema.RelationshipPropertyExistenceConstraintDefinition;
import org.neo4j.kernel.impl.coreapi.schema.UniquenessConstraintDefinition;

import static org.neo4j.function.Predicates.instanceOf;
import static org.neo4j.internal.helpers.collection.Iterables.filter;

public enum SchemaOperations implements SchemaOperation
{
    createIndex( SchemaOperationType.indexing )
            {
                @Override
                public void execute( GraphDatabaseService db, Transaction transaction )
                {
                    try
                    {
                        transaction.schema().indexFor( GraphOperations.randomLabel() ).on( GraphOperations.randomKey() ).create();
                    }
                    catch ( ConstraintViolationException e )
                    {
                        if ( e.getCause() instanceof AlreadyConstrainedException )
                        {
                            // This is expected when there is already a constraint on this label and property.
                            return;
                        }
                        else if ( e.getCause() instanceof AlreadyIndexedException )
                        {
                            // This is expected when there is already on index on this label.
                            return;
                        }
                        throw e;
                    }
                }
            },
    dropIndex( SchemaOperationType.indexing )
            {
                @Override
                public void execute( GraphDatabaseService db, Transaction transaction )
                {
                    IndexDefinition indexDefinition = GraphOperations.chooseRandomIndex( transaction.schema().getIndexes() );
                    if ( indexDefinition != null )
                    {
                        indexDefinition.drop();
                    }
                }
            },
    createUniquenessConstraint( SchemaOperationType.uniquenessConstraints )
            {
                @Override
                public void execute( GraphDatabaseService db, Transaction transaction )
                {
                    try
                    {
                        transaction.schema().constraintFor( GraphOperations.randomLabel() ).assertPropertyIsUnique( GraphOperations.randomKey() ).create();
                    }
                    catch ( ConstraintViolationException e )
                    {
                        handleConstraintViolationExceptionOnCreation( e );
                    }
                }
            },
    dropUniquenessConstraint( SchemaOperationType.uniquenessConstraints )
            {
                @Override
                public void execute( GraphDatabaseService db, Transaction transaction )
                {
                    ConstraintDefinition constraint = randomUniquenessConstraint( transaction );
                    if ( constraint != null )
                    {
                        constraint.drop();
                    }
                }
            },
    createNodeKeyConstraint( SchemaOperationType.nodeKeyConstraints )
            {
                @Override
                public void execute( GraphDatabaseService db, Transaction transaction )
                {
                    try
                    {
                        String prop1 = GraphOperations.randomKey();
                        transaction.execute(
                                String.format( "CREATE CONSTRAINT ON (n:%s) ASSERT (n.%s,n.%s) IS NODE KEY", GraphOperations.randomLabel().name(), prop1,
                                        GraphOperations.randomKeyButNot( prop1 ) ) );
                    }
                    catch ( ConstraintViolationException e )
                    {
                        handleConstraintViolationExceptionOnCreation( e );
                    }
                }
            },
    dropNodeKeyConstraint( SchemaOperationType.nodeKeyConstraints )
            {
                @Override
                public void execute( GraphDatabaseService db, Transaction transaction )
                {
                    ConstraintDefinition constraint = randomNodeKeyConstraint( transaction );
                    if ( constraint != null )
                    {
                        constraint.drop();
                    }
                }
            },
    createNodePropertyExistenceConstraint( SchemaOperationType.nodePropertyExistenceConstraints )
            {
                @Override
                public void execute( GraphDatabaseService db, Transaction transaction )
                {
                    try
                    {
                        transaction.execute( String.format( "CREATE CONSTRAINT ON (n:%s) ASSERT (n.%s) IS NOT NULL", GraphOperations.randomLabel().name(),
                                GraphOperations.randomKey() ) );
                    }
                    catch ( QueryExecutionException e )
                    {
                        handleConstraintViolationExceptionOnCreation( e );
                    }
                }
            },
    dropNodePropertyExistenceConstraint( SchemaOperationType.nodePropertyExistenceConstraints )
            {
                @Override
                public void execute( GraphDatabaseService db, Transaction transaction )
                {
                    ConstraintDefinition constraint = randomNodePropertyExistenceConstraint( transaction );
                    if ( constraint != null )
                    {
                        constraint.drop();
                    }
                }
            },
    createRelPropertyExistenceConstraint( SchemaOperationType.relationshipPropertyExistenceConstraints )
            {
                @Override
                public void execute( GraphDatabaseService db, Transaction transaction )
                {
                    try
                    {
                        transaction.execute( String.format( "CREATE CONSTRAINT ON ()-[r:%s]-() ASSERT (r.%s) IS NOT NULL",
                                GraphOperations.randomRelType().name(), GraphOperations.randomKey() ) );
                    }
                    catch ( QueryExecutionException e )
                    {
                        handleConstraintViolationExceptionOnCreation( e );
                    }
                }
            },
    dropRelPropertyExistenceConstraint( SchemaOperationType.relationshipPropertyExistenceConstraints )
            {
                @Override
                public void execute( GraphDatabaseService db, Transaction transaction )
                {
                    ConstraintDefinition constraint = randomRelationshipPropertyExistenceConstraint( transaction );
                    if ( constraint != null )
                    {
                        constraint.drop();
                    }
                }
            };

    private final SchemaOperationType type;

    SchemaOperations( SchemaOperationType type )
    {
        this.type = type;
    }

    private static void handleConstraintViolationExceptionOnCreation( Exception e )
    {
        if ( ExceptionUtils.indexOfThrowable( e, AlreadyIndexedException.class ) != -1 )
        {
            // This is expected when we try to create a constraint for a combination which already has an index.
        }
        else if ( ExceptionUtils.indexOfThrowable( e, AlreadyConstrainedException.class ) != -1 )
        {
            // This is expected when we try to create a duplicate constraint.
        }
        else if ( ExceptionUtils.indexOfThrowable( e, CreateConstraintFailureException.class ) != -1 )
        {
            Optional<Throwable> causeOrSuppressed = Exceptions.findCauseOrSuppressed( e,
                    cause -> cause.getMessage() != null && cause.getMessage().contains( "Existing data does not satisfy" ) );
            if ( causeOrSuppressed.isPresent() )
            {
                throw new RuntimeException( e );
            }
        }
    }

    private static UniquenessConstraintDefinition randomUniquenessConstraint( Transaction transaction )
    {
        return randomConstraint( transaction, UniquenessConstraintDefinition.class );
    }

    private static NodeKeyConstraintDefinition randomNodeKeyConstraint( Transaction transaction )
    {
        return randomConstraint( transaction, NodeKeyConstraintDefinition.class );
    }

    private static NodePropertyExistenceConstraintDefinition randomNodePropertyExistenceConstraint( Transaction transaction )
    {
        return randomConstraint( transaction, NodePropertyExistenceConstraintDefinition.class );
    }

    private static RelationshipPropertyExistenceConstraintDefinition randomRelationshipPropertyExistenceConstraint( Transaction transaction )
    {
        return randomConstraint( transaction, RelationshipPropertyExistenceConstraintDefinition.class );
    }

    private static <C extends ConstraintDefinition> C randomConstraint( Transaction transaction, Class<C> clazz )
    {
        return clazz.cast( GraphOperations.chooseRandom( filter( instanceOf( clazz ), transaction.schema().getConstraints() ) ) );
    }

    @Override
    public SchemaOperationType type()
    {
        return type;
    }
}
