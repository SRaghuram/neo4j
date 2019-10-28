/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.graphdb;

import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.SchemaAcceptanceTestBase;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintWithNameAlreadyExistsException;
import org.neo4j.kernel.api.exceptions.schema.EquivalentSchemaRuleAlreadyExistsException;
import org.neo4j.kernel.api.exceptions.schema.IndexWithNameAlreadyExistsException;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.schema.IndexType.BTREE;
import static org.neo4j.graphdb.schema.IndexType.FULLTEXT;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

@ExtendWith( RandomExtension.class )
@ImpermanentEnterpriseDbmsExtension
class SchemaAcceptanceEnterpriseTest extends SchemaAcceptanceTestBase
{
    @Inject
    private GraphDatabaseService db;
    @Inject
    private RandomRule random;

    @Test
    void shouldCreateNodePropertyExistenceConstraint()
    {
        // WHEN
        ConstraintDefinition constraint = createNodePropertyExistenceConstraint( label, propertyKey );

        // THEN
        try ( Transaction tx = db.beginTx() )
        {
            constraint = tx.schema().getConstraintByName( constraint.getName() );
            assertEquals( ConstraintType.NODE_PROPERTY_EXISTENCE, constraint.getConstraintType() );
            assertEquals( label.name(), constraint.getLabel().name() );
            assertEquals( asSet( propertyKey ), Iterables.asSet( constraint.getPropertyKeys() ) );
            assertEquals( "constraint_591c9882", constraint.getName() );
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
            constraint = tx.schema().getConstraintByName( constraint.getName() );
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
            constraint = tx.schema().getConstraintByName( constraint.getName() );
            assertEquals( ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE, constraint.getConstraintType() );
            assertEquals( relType.name(), constraint.getRelationshipType().name() );
            assertEquals( asSet( propertyKey ), Iterables.asSet( constraint.getPropertyKeys() ) );
            assertEquals( "constraint_4d78cae1", constraint.getName() );
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
            constraint = tx.schema().getConstraintByName( constraint.getName() );
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
            constraint = tx.schema().getConstraintByName( constraint.getName() );
            assertEquals( ConstraintType.NODE_KEY, constraint.getConstraintType() );
            assertEquals( label.name(), constraint.getLabel().name() );
            assertEquals( asSet( propertyKey ), Iterables.asSet( constraint.getPropertyKeys() ) );
            assertEquals( "constraint_bbf8bfaa", constraint.getName() );
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
            constraint = tx.schema().getConstraintByName( constraint.getName() );
            assertEquals( ConstraintType.NODE_KEY, constraint.getConstraintType() );
            assertEquals( label.name(), constraint.getLabel().name() );
            assertEquals( asSet( propertyKey ), Iterables.asSet( constraint.getPropertyKeys() ) );
            assertEquals( "MyConstraint", constraint.getName() );
            tx.commit();
        }
    }

    @Test
    void shouldCreateNodeKeyOnSameSchemaAsExistenceAndDropIndependently()
    {
        // WHEN
        ConstraintDefinition existenceConstraint = createNodePropertyExistenceConstraint( label, propertyKey );
        ConstraintDefinition nodeKeyConstraint = createNodeKeyConstraint( label, propertyKey );
        assertConstraintsExists( existenceConstraint, nodeKeyConstraint );

        final ConstraintDefinition theOther = dropOneConstraint( existenceConstraint, nodeKeyConstraint );
        assertConstraintsExists( theOther );
    }

    @Test
    void shouldCreateUniquenessOnSameSchemaAsExistenceAndDropIndependently()
    {
        // WHEN
        ConstraintDefinition existenceConstraint = createNodePropertyExistenceConstraint( label, propertyKey );
        ConstraintDefinition uniquenessConstraint = createUniquenessConstraint( label, propertyKey );
        assertConstraintsExists( existenceConstraint, uniquenessConstraint );

        final ConstraintDefinition theOther = dropOneConstraint( existenceConstraint, uniquenessConstraint );
        assertConstraintsExists( theOther );
    }

    @Test
    void shouldCreateExistenceOnSameSchemaAsNodeKeyAndDropIndependently()
    {
        // WHEN
        ConstraintDefinition nodeKeyConstraint = createNodeKeyConstraint( label, propertyKey );
        ConstraintDefinition existenceConstraint = createNodePropertyExistenceConstraint( label, propertyKey );
        assertConstraintsExists( existenceConstraint, nodeKeyConstraint );

        final ConstraintDefinition theOther = dropOneConstraint( existenceConstraint, nodeKeyConstraint );
        assertConstraintsExists( theOther );
    }

    @Test
    void shouldCreateExistenceOnSameSchemaAsUniquenessAndDropIndependently()
    {
        // WHEN
        ConstraintDefinition uniquenessConstraint = createUniquenessConstraint( label, propertyKey );
        ConstraintDefinition existenceConstraint = createNodePropertyExistenceConstraint( label, propertyKey );
        assertConstraintsExists( existenceConstraint, uniquenessConstraint );

        final ConstraintDefinition theOther = dropOneConstraint( existenceConstraint, uniquenessConstraint );
        assertConstraintsExists( theOther );
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
        String expectedMessage = "Constraint already exists: Constraint( UNIQUE, :MY_LABEL(my_property_key) )";
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
        String expectedMessage = "Constraint already exists: Constraint( UNIQUE_EXISTS, :MY_LABEL(my_property_key) )";
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
        String expectedMessage = "Constraint already exists: Constraint( UNIQUE_EXISTS, :MY_LABEL(my_property_key) )";
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

    @Test
    void nonIndexBackedConstraintNamesCannotContainBackTicks()
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintCreator creator = tx.schema().constraintFor( label ).withName( "a`b" ).assertPropertyExists( propertyKey );
            assertThrows( IllegalArgumentException.class, creator::create );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( count( tx.schema().getIndexes() ), is( 0L ) );
            assertThat( count( tx.schema().getConstraints() ), is( 0L ) );
            tx.commit();
        }
    }

    @Test
    void nodeKeyConstraintIndexesMustHaveBtreeIndexTypeByDefault()
    {
        String name;
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintDefinition constraint = tx.schema().constraintFor( label ).assertPropertyIsNodeKey( propertyKey ).create();
            name = constraint.getName();
            IndexDefinition index = tx.schema().getIndexByName( name );
            assertThat( index.getIndexType(), is( BTREE ) );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().getIndexByName( name );
            assertThat( index.getIndexType(), is( BTREE ) );
        }
    }

    @Test
    void mustBeAbleToCreateNodeKeyConstraintWithBtreeIndexType()
    {
        String name;
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintDefinition constraint = tx.schema().constraintFor( label ).assertPropertyIsNodeKey( propertyKey ).withIndexType( BTREE ).create();
            name = constraint.getName();
            IndexDefinition index = tx.schema().getIndexByName( name );
            assertThat( index.getIndexType(), is( BTREE ) );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().getIndexByName( name );
            assertThat( index.getIndexType(), is( BTREE ) );
        }
    }

    @Test
    void creatingNodeKeyConstraintWithFullTextIndexTypeMustThrow()
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintCreator creator = tx.schema().constraintFor( label ).assertPropertyIsNodeKey( propertyKey ).withIndexType( FULLTEXT );
            assertThrows( IllegalArgumentException.class, creator::create );
            tx.commit();
        }
    }

    @Test
    void creatingNodePropertyExistenceConstraintMustThrowWhenGivenIndexType()
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintCreator creator = tx.schema().constraintFor( label ).assertPropertyExists( propertyKey ).withIndexType( BTREE );
            assertThrows( IllegalArgumentException.class, creator::create );
            tx.commit();
        }
    }

    @Test
    void creatingRelationshipPropertyExistenceConstraintMustThrowWhenGivenIndexType()
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintCreator creator = tx.schema().constraintFor( relType ).assertPropertyExists( propertyKey ).withIndexType( BTREE );
            assertThrows( IllegalArgumentException.class, creator::create );
            tx.commit();
        }
    }
    // todo must be able to create node key constraint with index configuration
    // todo creating node property existence constraints must throw when given index configuration
    // todo creating relationship property existence constraints must throw when given index configuration

    private ConstraintDefinition createUniquenessConstraint( Label label, String prop )
    {
        return createUniquenessConstraint( null, label, prop );
    }

    private ConstraintDefinition createUniquenessConstraint( String name, Label label, String prop )
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintCreator creator = tx.schema().constraintFor( label );
            creator = creator.assertPropertyIsUnique( prop ).withName( name );
            ConstraintDefinition constraint = creator.create();
            tx.commit();
            return constraint;
        }
    }

    private ConstraintDefinition createNodeKeyConstraint( Label label, String prop )
    {
        return createNodeKeyConstraint( null, label, prop );
    }

    private ConstraintDefinition createNodeKeyConstraint( String name, Label label, String prop )
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintCreator creator = tx.schema().constraintFor( label );
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
            ConstraintCreator creator = tx.schema().constraintFor( label );
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
            ConstraintCreator creator = tx.schema().constraintFor( relType );
            creator = creator.assertPropertyExists( prop ).withName( name );
            ConstraintDefinition constraint = creator.create();
            tx.commit();
            return constraint;
        }
    }

    private ConstraintDefinition dropOneConstraint( ConstraintDefinition constraint1, ConstraintDefinition constraint2 )
    {
        boolean drop1 = random.nextBoolean();
        try ( Transaction tx = db.beginTx() )
        {
            if ( drop1 )
            {
                tx.schema().getConstraintByName( constraint1.getName() ).drop();
            }
            else
            {
                tx.schema().getConstraintByName( constraint2.getName() ).drop();
            }
            tx.commit();
        }
        return drop1 ? constraint2 : constraint1;
    }

    private void assertConstraintsExists( ConstraintDefinition... expectedConstraints )
    {
        try ( Transaction tx = db.beginTx() )
        {
            final List<ConstraintDefinition> allConstraints = Iterables.asList( tx.schema().getConstraints( label ) );
            for ( ConstraintDefinition expectedConstraint : expectedConstraints )
            {
                assertTrue( allConstraints.remove( expectedConstraint ), "Constraints did not contain " + expectedConstraint );
            }
            assertTrue( allConstraints.isEmpty(), "Expected no more constraints to exist but had " + allConstraints );
            tx.commit();
        }
    }
}
