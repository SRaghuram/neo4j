/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.graphdb;

import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.SchemaAcceptanceTestBase;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.EquivalentSchemaRuleAlreadyExistsException;
import org.neo4j.test.extension.Inject;

@ImpermanentEnterpriseDbmsExtension
class SchemaAcceptanceEnterpriseTest extends SchemaAcceptanceTestBase
{
    @Inject
    private GraphDatabaseService db;

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

    //todo
    // X shouldThrowIfEquivalentNodeKeyConstraintExist
    // X shouldThrowIfEquivalentExistenceConstraintExist
    // X shouldThrowIfSchemaAlreadyIndexedWhenCreatingNodeKeyConstraint
    // X shouldThrowIfSchemaAlreadyUniquenessConstrainedWhenCreatingNodeKeyConstraint
    // X shouldThrowIfSchemaAlreadyNodeKeyConstrainedWhenCreatingIndex
    // X shouldThrowIfSchemaAlreadyNodeKeyConstrainedWhenCreatingUniquenessConstraint
    // X shouldThrowIfSchemaAlreadyNodeKeyConstrainedWhenCreatingNodeKeyConstraint
}
