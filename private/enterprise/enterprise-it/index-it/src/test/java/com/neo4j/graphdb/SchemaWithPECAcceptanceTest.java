/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.graphdb;

import com.neo4j.SchemaHelper;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.helpers.collection.Iterables.asList;

@EnterpriseDbmsExtension
class SchemaWithPECAcceptanceTest
{
    @Inject
    private GraphDatabaseService db;

    private final Label label = Labels.MY_LABEL;
    private final Label label2 = Labels.MY_OTHER_LABEL;
    private final String propertyKey = "my_property_key";
    private final String propertyKey2 = "my_other_property";

    private enum Labels implements Label
    {
        MY_LABEL,
        MY_OTHER_LABEL
    }

    private enum Types implements RelationshipType
    {
        MY_TYPE,
        MY_OTHER_TYPE
    }

    @ParameterizedTest
    @EnumSource( SchemaHelper.class )
    void shouldCreateNodePropertyExistenceConstraint( SchemaHelper helper )
    {
        // When
        ConstraintDefinition constraint = createNodePropertyExistenceConstraint( helper, label, propertyKey );

        // Then
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( getConstraints( transaction ) ).containsOnly( constraint );
        }
        assertExpectedConstraint( constraint, ConstraintType.NODE_PROPERTY_EXISTENCE, label, propertyKey );
    }

    @ParameterizedTest
    @EnumSource( SchemaHelper.class )
    void shouldCreateRelationshipPropertyExistenceConstraint( SchemaHelper helper )
    {
        // When
        ConstraintDefinition constraint = createRelationshipPropertyExistenceConstraint( helper, Types.MY_TYPE, propertyKey );

        // Then
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( getConstraints( transaction ) ).containsOnly( constraint );
        }
        assertExpectedConstraint( constraint, ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE, Types.MY_TYPE, propertyKey );
    }

    @ParameterizedTest
    @EnumSource( SchemaHelper.class )
    void shouldListAddedConstraintsByLabel( SchemaHelper helper )
    {
        // GIVEN
        ConstraintDefinition constraint1 = createUniquenessConstraint( helper, label, propertyKey );
        ConstraintDefinition constraint2 = createNodePropertyExistenceConstraint( helper, label, propertyKey );
        ConstraintDefinition constraint3 = createNodeKeyConstraint( helper, label, propertyKey2 );
        createNodeKeyConstraint( helper, label2, propertyKey2 );
        createNodePropertyExistenceConstraint( helper, Labels.MY_OTHER_LABEL, propertyKey );

        // WHEN THEN
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( getConstraints( transaction, label ) ).containsOnly( constraint1, constraint2, constraint3 );
        }
    }

    @ParameterizedTest
    @EnumSource( SchemaHelper.class )
    void shouldListAddedConstraintsByRelationshipType( SchemaHelper helper )
    {
        // GIVEN
        ConstraintDefinition constraint1 = createRelationshipPropertyExistenceConstraint( helper, Types.MY_TYPE, propertyKey );
        createRelationshipPropertyExistenceConstraint( helper, Types.MY_OTHER_TYPE, propertyKey );

        // WHEN THEN
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( getConstraints( transaction, Types.MY_TYPE ) ).containsOnly( constraint1 );
        }
    }

    @ParameterizedTest
    @EnumSource( SchemaHelper.class )
    void shouldListAddedConstraints( SchemaHelper helper )
    {
        // GIVEN
        ConstraintDefinition constraint1 = createUniquenessConstraint( helper, label, propertyKey );
        ConstraintDefinition constraint2 = createNodePropertyExistenceConstraint( helper, label, propertyKey );
        ConstraintDefinition constraint3 = createRelationshipPropertyExistenceConstraint( helper, Types.MY_TYPE, propertyKey );
        ConstraintDefinition constraint4 = createNodeKeyConstraint( helper, label, propertyKey2 );

        // WHEN THEN
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( getConstraints( transaction ) ).containsOnly( constraint1, constraint2, constraint3, constraint4 );
        }
    }

    @ParameterizedTest
    @EnumSource( SchemaHelper.class )
    void uniquenessConstraintIndexesMustBeNamedAfterTheirConstraints( SchemaHelper helper )
    {
        final ConstraintDefinition constraint = createNodeKeyConstraint( helper, "MySchema", label, propertyKey );
        assertExpectedConstraint( constraint, "MySchema", ConstraintType.NODE_KEY, label, propertyKey );
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().getIndexByName( "MySchema" );
            assertTrue( index.isConstraintIndex() );
            assertTrue( index.isNodeIndex() );
            assertEquals( "MySchema", index.getName() );
            tx.commit();
        }
    }

    private ConstraintDefinition createUniquenessConstraint( SchemaHelper helper, Label label, String propertyKey )
    {
        Iterable<ConstraintDefinition> before;
        try ( Transaction transaction = db.beginTx() )
        {
            before = getConstraints( transaction );
            helper.createUniquenessConstraint( db, transaction, label, propertyKey );
            transaction.commit();
        }
        helper.awaitIndexes( db );
        try ( Transaction transaction = db.beginTx() )
        {
            return getCreatedConstraint( transaction, before );
        }
    }

    private ConstraintDefinition getCreatedConstraint( Transaction transaction, Iterable<ConstraintDefinition> before )
    {
        var afterConstraints = asList( getConstraints( transaction ) );
        afterConstraints.removeAll( asList( before ) );
        if ( afterConstraints.size() == 1 )
        {
            return afterConstraints.iterator().next();
        }
        return fail( "Expected to only find a single constraint in the after set, but found " + afterConstraints );
    }

    private ConstraintDefinition createNodeKeyConstraint( SchemaHelper helper, Label label, String propertyKey )
    {
        Iterable<ConstraintDefinition> before;
        try ( Transaction transaction = db.beginTx() )
        {
            before = getConstraints( transaction );
            helper.createNodeKeyConstraint( db, transaction, label, propertyKey );
            transaction.commit();
        }

        helper.awaitIndexes( db );

        try ( Transaction transaction = db.beginTx() )
        {
            return getCreatedConstraint( transaction, before );
        }
    }

    private ConstraintDefinition createNodeKeyConstraint( SchemaHelper helper, String name, Label label, String propertyKey )
    {
        Iterable<ConstraintDefinition> before;
        try ( Transaction transaction = db.beginTx() )
        {
            before = getConstraints( transaction );
            helper.createNodeKeyConstraintWithName( transaction, name, label, propertyKey );
            transaction.commit();
        }

        helper.awaitIndexes( db );

        try ( Transaction transaction = db.beginTx() )
        {
            return getCreatedConstraint( transaction, before );
        }
    }

    private ConstraintDefinition createNodePropertyExistenceConstraint( SchemaHelper helper, Label label, String propertyKey )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Iterable<ConstraintDefinition> before = getConstraints( transaction );
            helper.createNodePropertyExistenceConstraint( db, transaction, label, propertyKey );
            var constraint = getCreatedConstraint( transaction, before );
            transaction.commit();
            return constraint;
        }
    }

    private ConstraintDefinition createRelationshipPropertyExistenceConstraint( SchemaHelper helper, Types type, String propertyKey )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Iterable<ConstraintDefinition> before = getConstraints( transaction );
            helper.createRelPropertyExistenceConstraint( db, transaction, type, propertyKey );
            var constraint = getCreatedConstraint( transaction, before );
            transaction.commit();
            return constraint;
        }
    }

    private void assertExpectedConstraint( ConstraintDefinition constraint, ConstraintType constraintType, RelationshipType expectedType,
            String... expectedProperties )
    {
        try ( Transaction tx = db.beginTx() )
        {
            constraint = tx.schema().getConstraintByName( constraint.getName() );
            List<String> propertyKeys = new ArrayList<>( Arrays.asList( expectedProperties ) );
            assertEquals( expectedType.name(), constraint.getRelationshipType().name() );
            assertEquals( propertyKeys, asList( constraint.getPropertyKeys() ) );
            assertEquals( constraintType, constraint.getConstraintType() );
            tx.commit();
        }
    }

    private void assertExpectedConstraint( ConstraintDefinition constraint, ConstraintType constraintType, Label expectedLabel, String... expectedProperties )
    {
        try ( Transaction tx = db.beginTx() )
        {
            constraint = tx.schema().getConstraintByName( constraint.getName() );
            List<String> propertyKeys = new ArrayList<>( Arrays.asList( expectedProperties ) );
            assertEquals( expectedLabel.name(), constraint.getLabel().name() );
            assertEquals( propertyKeys, asList( constraint.getPropertyKeys() ) );
            assertEquals( constraintType, constraint.getConstraintType() );
            tx.commit();
        }
    }

    private void assertExpectedConstraint( ConstraintDefinition constraint, String name, ConstraintType constraintType, Label expectedLabel,
            String... expectedProperties )
    {
        try ( Transaction tx = db.beginTx() )
        {
            constraint = tx.schema().getConstraintByName( constraint.getName() );
            List<String> propertyKeys = new ArrayList<>( Arrays.asList( expectedProperties ) );
            assertEquals( name, constraint.getName() );
            assertEquals( expectedLabel.name(), constraint.getLabel().name() );
            assertEquals( propertyKeys, asList( constraint.getPropertyKeys() ) );
            assertEquals( constraintType, constraint.getConstraintType() );
            tx.commit();
        }
    }

    private static Iterable<ConstraintDefinition> getConstraints( Transaction transaction, Label label )
    {
        return transaction.schema().getConstraints( label );
    }

    private static Iterable<ConstraintDefinition> getConstraints( Transaction transaction, RelationshipType relationshipType )
    {
        return transaction.schema().getConstraints( relationshipType );
    }

    private static Iterable<ConstraintDefinition> getConstraints( Transaction transaction )
    {
        return transaction.schema().getConstraints();
    }
}
