/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.api.integrationtest;

import com.neo4j.SchemaHelper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.schema.IndexPrototype;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.test.assertion.Assert.assertException;

class NodeKeyConstraintValidationIT extends NodePropertyExistenceConstraintValidationIT
{
    @Override
    void createConstraint( String key, String property ) throws KernelException
    {
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        int label = tokenWrite.labelGetOrCreateForName( key );
        int propertyKey = tokenWrite.propertyKeyGetOrCreateForName( property );
        commit();

        SchemaWrite schemaWrite = schemaWriteInNewTransaction();
        schemaWrite.nodeKeyConstraintCreate( IndexPrototype.uniqueForSchema( forLabel( label, propertyKey ) ) );
        commit();
    }

    @ParameterizedTest
    @EnumSource( SchemaHelper.class )
    void requirePropertyFromMultipleNodeKeys( SchemaHelper helper )
    {
        Label label = Label.label( "multiNodeKeyLabel" );
        try ( org.neo4j.graphdb.Transaction transaction = db.beginTx() )
        {
            helper.createNodeKeyConstraint( db, transaction, label, "property1", "property2" );
            helper.createNodeKeyConstraint( db, transaction, label, "property2", "property3" );
            helper.createNodeKeyConstraint( db, transaction, label, "property3", "property4" );
            transaction.commit();
        }

        assertException( () ->
        {
            try ( org.neo4j.graphdb.Transaction transaction = db.beginTx() )
            {
                Node node = transaction.createNode( label );
                node.setProperty( "property1", "1" );
                node.setProperty( "property2", "2" );
                transaction.commit();
            }
        }, ConstraintViolationException.class,
                anyOf( containsString( "with label `multiNodeKeyLabel` must have the properties (property2, property3)" ),
                        containsString( "with label `multiNodeKeyLabel` must have the properties (property3, property4)" ) ) );

        assertException( () ->
        {
            try ( org.neo4j.graphdb.Transaction transaction = db.beginTx() )
            {
                Node node = transaction.createNode( label );
                node.setProperty( "property1", "1" );
                node.setProperty( "property2", "2" );
                node.setProperty( "property3", "3" );
                transaction.commit();
            }
        }, ConstraintViolationException.class, containsString( "with label `multiNodeKeyLabel` must have the properties (property3, property4)" ) );
    }
}
