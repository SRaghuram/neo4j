/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.graphdb;

import com.neo4j.SchemaHelper;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collection;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.test.extension.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.containsOnly;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.getConstraints;

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
            assertThat( getConstraints( transaction ), containsOnly( constraint ) );
        }
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
            assertThat( getConstraints( transaction ), containsOnly( constraint ) );
        }
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
            assertThat( getConstraints( transaction, label ), containsOnly( constraint1, constraint2, constraint3 ) );
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
            assertThat( getConstraints( transaction, Types.MY_TYPE ), containsOnly( constraint1 ) );
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
            assertThat( getConstraints( transaction ), containsOnly( constraint1, constraint2, constraint3, constraint4 ) );
        }
    }

    @ParameterizedTest
    @EnumSource( SchemaHelper.class )
    void uniquenessConstraintIndexesMustBeNamedAfterTheirConstraints( SchemaHelper helper )
    {
        createNodeKeyConstraint( helper, "MySchema", label, propertyKey );
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
        Collection<ConstraintDefinition> before;
        try ( Transaction transaction = db.beginTx() )
        {
            before = getConstraints( transaction ).collection();
            helper.createUniquenessConstraint( db, transaction, label, propertyKey );
            transaction.commit();
        }
        helper.awaitIndexes( db );
        try ( Transaction transaction = db.beginTx() )
        {
            return getCreatedConstraint( transaction, before );
        }
    }

    private ConstraintDefinition getCreatedConstraint( Transaction transaction, Collection<ConstraintDefinition> before )
    {
        Collection<ConstraintDefinition> after = getConstraints( transaction ).collection();
        after.removeAll( before );
        if ( after.size() == 1 )
        {
            return after.iterator().next();
        }
        return fail( "Expected to only find a single constraint in the after set, but found " + after );
    }

    private ConstraintDefinition createNodeKeyConstraint( SchemaHelper helper, Label label, String propertyKey )
    {
        Collection<ConstraintDefinition> before;
        try ( Transaction transaction = db.beginTx() )
        {
            before = getConstraints( transaction ).collection();
            helper.createNodeKeyConstraint( db, transaction, label, propertyKey );
            transaction.commit();
        }

        helper.awaitIndexes( db );

        try ( Transaction transaction = db.beginTx() )
        {
            return getCreatedConstraint( transaction, before );
        }
    }

    private void createNodeKeyConstraint( SchemaHelper helper, String name, Label label, String propertyKey )
    {
        Collection<ConstraintDefinition> before;
        ConstraintDefinition constraint;
        try ( Transaction transaction = db.beginTx() )
        {
            before = getConstraints( transaction ).collection();
            constraint = helper.createNodeKeyConstraint( transaction, name, label, propertyKey );
            transaction.commit();
        }

        helper.awaitIndexes( db );

        try ( Transaction transaction = db.beginTx() )
        {
            ConstraintDefinition foundConstraint = getCreatedConstraint( transaction, before );
            assertEquals( constraint, foundConstraint );
        }
    }

    private ConstraintDefinition createNodePropertyExistenceConstraint( SchemaHelper helper, Label label, String propertyKey )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Collection<ConstraintDefinition> before = getConstraints( transaction ).collection();
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
            Collection<ConstraintDefinition> before = getConstraints( transaction ).collection();
            helper.createRelPropertyExistenceConstraint( db, transaction, type, propertyKey );
            var constraint = getCreatedConstraint( transaction, before );
            transaction.commit();
            return constraint;
        }
    }
}
