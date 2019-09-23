/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.graphdb;

import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.SchemaAcceptanceTestBase;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintWithNameAlreadyExistsException;
import org.neo4j.kernel.api.exceptions.schema.EquivalentSchemaRuleAlreadyExistsException;
import org.neo4j.kernel.api.exceptions.schema.IndexWithNameAlreadyExistsException;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

@ImpermanentEnterpriseDbmsExtension
class SchemaAcceptanceEnterpriseTest extends SchemaAcceptanceTestBase
{
    @Inject
    private GraphDatabaseService db;

    @Test
    void shouldCreateNodePropertyExistenceConstraint()
    {
        // WHEN
        ConstraintDefinition constraint = createNodePropertyExistenceConstraint( label, propertyKey );

        // THEN
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( ConstraintType.NODE_PROPERTY_EXISTENCE, constraint.getConstraintType() );
            assertEquals( label.name(), constraint.getLabel().name() );
            assertEquals( asSet( propertyKey ), Iterables.asSet( constraint.getPropertyKeys() ) );
            assertEquals( "Property existence constraint on :MY_LABEL (my_property_key)", constraint.getName() );
            tx.commit();
        }
    }

    @Test
    void shouldCreateNamedNodePropertyExistenceConstraint()
    {
        // When
        ConstraintDefinition constraint = createNodePropertyExistenceConstraint( "MyConstraint", label, propertyKey );

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( ConstraintType.NODE_PROPERTY_EXISTENCE, constraint.getConstraintType() );
            assertEquals( label.name(), constraint.getLabel().name() );
            assertEquals( asSet( propertyKey ), Iterables.asSet( constraint.getPropertyKeys() ) );
            assertEquals( "MyConstraint", constraint.getName() );
            tx.commit();
        }
    }

    @Test
    void shouldCreateRelationshipPropertyExistenceConstraint()
    {
        // WHEN
        ConstraintDefinition constraint = createRelationshipPropertyExistenceConstraint( relType, propertyKey );

        // THEN
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE, constraint.getConstraintType() );
            assertEquals( relType.name(), constraint.getRelationshipType().name() );
            assertEquals( asSet( propertyKey ), Iterables.asSet( constraint.getPropertyKeys() ) );
            assertEquals( "Property existence constraint on ()-[:relType]-() (my_property_key)", constraint.getName() );
            tx.commit();
        }
    }

    @Test
    void shouldCreateNamedRelationshipPropertyExistenceConstraint()
    {
        // When
        ConstraintDefinition constraint = createRelationshipPropertyExistenceConstraint( "MyConstraint", relType, propertyKey );

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE, constraint.getConstraintType() );
            assertEquals( relType.name(), constraint.getRelationshipType().name() );
            assertEquals( asSet( propertyKey ), Iterables.asSet( constraint.getPropertyKeys() ) );
            assertEquals( "MyConstraint", constraint.getName() );
            tx.commit();
        }
    }

    @Test
    void shouldCreateNodeKeyConstraint()
    {
        // WHEN
        ConstraintDefinition constraint = createNodeKeyConstraint( label, propertyKey );

        // THEN
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( ConstraintType.NODE_KEY, constraint.getConstraintType() );
            assertEquals( label.name(), constraint.getLabel().name() );
            assertEquals( asSet( propertyKey ), Iterables.asSet( constraint.getPropertyKeys() ) );
            assertEquals( "Node key constraint on :MY_LABEL(my_property_key)", constraint.getName() );
            tx.commit();
        }
    }

    @Test
    void shouldCreateNamedNodeKeyConstraint()
    {
        // When
        ConstraintDefinition constraint = createNodeKeyConstraint( "MyConstraint", label, propertyKey );

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( ConstraintType.NODE_KEY, constraint.getConstraintType() );
            assertEquals( label.name(), constraint.getLabel().name() );
            assertEquals( asSet( propertyKey ), Iterables.asSet( constraint.getPropertyKeys() ) );
            assertEquals( "MyConstraint", constraint.getName() );
            tx.commit();
        }
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfEquivalentNodeKeyConstraintExist( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.constraintFor( label ).assertPropertyIsNodeKey( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.constraintFor( label ).assertPropertyIsNodeKey( propertyKey ).withName( "name" ).create(),
                ConstraintViolationException.class );
        Class<EquivalentSchemaRuleAlreadyExistsException> expectedCause = EquivalentSchemaRuleAlreadyExistsException.class;
        String expectedMessage = "An equivalent constraint already exists, 'Constraint( UNIQUE_EXISTS, :MY_LABEL(my_property_key) )'.";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfEquivalentExistenceConstraintExist( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.constraintFor( label ).assertPropertyExists( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.constraintFor( label ).assertPropertyExists( propertyKey ).withName( "name" ).create(),
                ConstraintViolationException.class );
        Class<EquivalentSchemaRuleAlreadyExistsException> expectedCause = EquivalentSchemaRuleAlreadyExistsException.class;
        String expectedMessage = "An equivalent constraint already exists, 'Constraint( EXISTS, :MY_LABEL(my_property_key) )'.";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfEquivalentRelationshipExistenceConstraintExist( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.constraintFor( relType ).assertPropertyExists( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.constraintFor( relType ).assertPropertyExists( propertyKey ).withName( "name" ).create(),
                ConstraintViolationException.class );
        Class<EquivalentSchemaRuleAlreadyExistsException> expectedCause = EquivalentSchemaRuleAlreadyExistsException.class;
        String expectedMessage = "An equivalent constraint already exists, 'Constraint( EXISTS, -[:relType(my_property_key)]- )'.";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfSchemaAlreadyIndexedWhenCreatingNodeKeyConstraint( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.indexFor( label ).on( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.constraintFor( label ).assertPropertyIsNodeKey( propertyKey ).withName( "otherName" ).create(),
                ConstraintViolationException.class );
        Class<AlreadyIndexedException> expectedCause = AlreadyIndexedException.class;
        String expectedMessage = "There already exists an index :MY_LABEL(my_property_key). A constraint cannot be created until the index has been dropped.";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfSchemaAlreadyUniquenessConstrainedWhenCreatingNodeKeyConstraint( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.constraintFor( label ).assertPropertyIsUnique( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.constraintFor( label ).assertPropertyIsNodeKey( propertyKey ).withName( "otherName" ).create(),
                ConstraintViolationException.class );
        Class<AlreadyConstrainedException> expectedCause = AlreadyConstrainedException.class;
        String expectedMessage = "Constraint already exists: CONSTRAINT ON ( my_label:MY_LABEL ) ASSERT (my_label.my_property_key) IS UNIQUE";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfSchemaAlreadyNodeKeyConstrainedWhenCreatingIndex( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.constraintFor( label ).assertPropertyIsNodeKey( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.indexFor( label ).on( propertyKey ).withName( "otherName" ).create(),
                ConstraintViolationException.class );
        Class<AlreadyConstrainedException> expectedCause = AlreadyConstrainedException.class;
        String expectedMessage = "There is a uniqueness constraint on :MY_LABEL(my_property_key), so an index is already created that matches this.";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfSchemaAlreadyNodeKeyConstrainedWhenCreatingUniquenessConstraint( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.constraintFor( label ).assertPropertyIsNodeKey( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.constraintFor( label ).assertPropertyIsUnique( propertyKey ).withName( "otherName" ).create(),
                ConstraintViolationException.class );
        Class<AlreadyConstrainedException> expectedCause = AlreadyConstrainedException.class;
        String expectedMessage = "Constraint already exists: CONSTRAINT ON ( my_label:MY_LABEL ) ASSERT (my_label.my_property_key) IS NODE KEY";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfSchemaAlreadyNodeKeyConstrainedWhenCreatingNodeKeyConstraint( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.constraintFor( label ).assertPropertyIsNodeKey( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.constraintFor( label ).assertPropertyIsNodeKey( propertyKey ).withName( "otherName" ).create(),
                ConstraintViolationException.class );
        Class<AlreadyConstrainedException> expectedCause = AlreadyConstrainedException.class;
        String expectedMessage = "Constraint already exists: CONSTRAINT ON ( my_label:MY_LABEL ) ASSERT (my_label.my_property_key) IS NODE KEY";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfIndexWithNameExistsWhenCreatingNodeKeyConstraint( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.indexFor( label ).on( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.constraintFor( label ).assertPropertyIsNodeKey( secondPropertyKey ).withName( "name" ).create(),
                ConstraintViolationException.class );
        Class<IndexWithNameAlreadyExistsException> expectedCause = IndexWithNameAlreadyExistsException.class;
        String expectedMessage = "There already exists an index called 'name'.";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfIndexWithNameExistsWhenCreatingExistenceConstraint( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.indexFor( label ).on( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.constraintFor( label ).assertPropertyExists( secondPropertyKey ).withName( "name" ).create(),
                ConstraintViolationException.class );
        Class<IndexWithNameAlreadyExistsException> expectedCause = IndexWithNameAlreadyExistsException.class;
        String expectedMessage = "There already exists an index called 'name'.";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfIndexWithNameExistsWhenCreatingExistenceConstraintOnRelationship( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.indexFor( label ).on( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.constraintFor( relType ).assertPropertyExists( secondPropertyKey ).withName( "name" ).create(),
                ConstraintViolationException.class );
        Class<IndexWithNameAlreadyExistsException> expectedCause = IndexWithNameAlreadyExistsException.class;
        String expectedMessage = "There already exists an index called 'name'.";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfConstraintWithNameExistsWhenCreatingNodeKeyConstraint( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.constraintFor( label ).assertPropertyIsUnique( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.constraintFor( label ).assertPropertyIsNodeKey( secondPropertyKey ).withName( "name" ).create(),
                ConstraintViolationException.class );
        Class<ConstraintWithNameAlreadyExistsException> expectedCause = ConstraintWithNameAlreadyExistsException.class;
        String expectedMessage = "There already exists a constraint called 'name'.";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfConstraintWithNameExistsWhenCreatingExistenceConstraint( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.constraintFor( label ).assertPropertyIsUnique( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.constraintFor( label ).assertPropertyExists( secondPropertyKey ).withName( "name" ).create(),
                ConstraintViolationException.class );
        Class<ConstraintWithNameAlreadyExistsException> expectedCause = ConstraintWithNameAlreadyExistsException.class;
        String expectedMessage = "There already exists a constraint called 'name'.";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfConstraintWithNameExistsWhenCreatingExistenceConstraintOnRelationship( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.constraintFor( label ).assertPropertyIsUnique( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.constraintFor( relType ).assertPropertyExists( secondPropertyKey ).withName( "name" ).create(),
                ConstraintViolationException.class );
        Class<ConstraintWithNameAlreadyExistsException> expectedCause = ConstraintWithNameAlreadyExistsException.class;
        String expectedMessage = "There already exists a constraint called 'name'.";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    private ConstraintDefinition createNodeKeyConstraint( Label label, String prop )
    {
        return createNodeKeyConstraint( null, label, prop );
    }

    private ConstraintDefinition createNodeKeyConstraint( String name, Label label, String prop )
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintCreator creator = db.schema().constraintFor( label );
            creator = creator.assertPropertyIsNodeKey( prop ).withName( name );
            ConstraintDefinition constraint = creator.create();
            tx.commit();
            return constraint;
        }
    }

    private ConstraintDefinition createNodePropertyExistenceConstraint( Label label, String prop )
    {
        return createNodePropertyExistenceConstraint( null, label, prop );
    }

    private ConstraintDefinition createNodePropertyExistenceConstraint( String name, Label label, String prop )
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintCreator creator = db.schema().constraintFor( label );
            creator = creator.assertPropertyExists( prop ).withName( name );
            ConstraintDefinition constraint = creator.create();
            tx.commit();
            return constraint;
        }
    }

    private ConstraintDefinition createRelationshipPropertyExistenceConstraint( RelationshipType relType, String prop )
    {
        return createRelationshipPropertyExistenceConstraint( null, relType, prop );
    }

    private ConstraintDefinition createRelationshipPropertyExistenceConstraint( String name, RelationshipType relType, String prop )
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintCreator creator = db.schema().constraintFor( relType );
            creator = creator.assertPropertyExists( prop ).withName( name );
            ConstraintDefinition constraint = creator.create();
            tx.commit();
            return constraint;
        }
    }
}
