/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import org.opentest4j.TestAbortedException;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public enum SchemaHelper
{
    CYPHER
            {
                @Override
                public void createIndex( GraphDatabaseService db, Transaction tx, String label, String property )
                {
                    tx.execute( format( "CREATE INDEX FOR (n:`%s`) ON (n.`%s`)", label, property ) );
                }

                @Override
                public void createUniquenessConstraint( GraphDatabaseService db, Transaction tx, String label, String property )
                {
                    tx.execute( format( "CREATE CONSTRAINT ON (n:`%s`) ASSERT n.`%s` IS UNIQUE", label, property ) );
                }

                @Override
                public void createNodeKeyConstraint( GraphDatabaseService db, Transaction tx, Label label, String... properties )
                {
                    String keyProperties = Arrays.stream( properties )
                            .map( property -> format("n.`%s`", property))
                            .collect( joining( "," ) );
                    tx.execute( format( "CREATE CONSTRAINT ON (n:`%s`) ASSERT (%s) IS NODE KEY", label.name(), keyProperties ) );
                }

                @Override
                public void createNodePropertyExistenceConstraint( GraphDatabaseService db, Transaction tx, String label, String property )
                {
                    tx.execute( format( "CREATE CONSTRAINT ON (n:`%s`) ASSERT exists(n.`%s`)", label, property ) );
                }

                @Override
                public void createRelPropertyExistenceConstraint( GraphDatabaseService db, Transaction tx, String type, String property )
                {
                    tx.execute( format( "CREATE CONSTRAINT ON ()-[r:`%s`]-() ASSERT exists(r.`%s`)", type, property ) );
                }

                @Override
                public ConstraintDefinition createNodeKeyConstraint( Transaction tx, String name, Label label, String... propertyKey )
                {
                    throw new TestAbortedException( "Cypher cannot yet create NAMED node key constraints." );
                }
            },
    CORE_API
            {
                @Override
                public void createIndex( GraphDatabaseService db, Transaction tx, String label, String property )
                {
                    tx.schema().indexFor( Label.label( label ) ).on( property ).create();
                }

                @Override
                public void createUniquenessConstraint( GraphDatabaseService db, Transaction tx, String label, String property )
                {
                    tx.schema().constraintFor( Label.label( label ) ).assertPropertyIsUnique( property ).create();
                }

                @Override
                public void createNodeKeyConstraint( GraphDatabaseService db, Transaction tx, Label label, String... properties )
                {
                    ConstraintCreator creator = tx.schema().constraintFor( label );
                    for ( String property : properties )
                    {
                        creator = creator.assertPropertyIsNodeKey( property );
                    }
                    creator.create();
                }

                @Override
                public void createNodePropertyExistenceConstraint( GraphDatabaseService db, Transaction tx, String label, String property )
                {
                    tx.schema().constraintFor( Label.label( label ) ).assertPropertyExists( property ).create();
                }

                @Override
                public void createRelPropertyExistenceConstraint( GraphDatabaseService db, Transaction tx, String type, String property )
                {
                    tx.schema().constraintFor( RelationshipType.withName( type ) ).assertPropertyExists( property ).create();
                }

                @Override
                public ConstraintDefinition createNodeKeyConstraint( Transaction tx, String name, Label label, String... keys )
                {
                    ConstraintCreator creator = tx.schema().constraintFor( label ).withName( name );
                    for ( String key : keys )
                    {
                        creator = creator.assertPropertyIsNodeKey( key );
                    }
                    return creator.create();
                }
            };

    public abstract void createIndex( GraphDatabaseService db, Transaction tx, String label, String property );

    public void createUniquenessConstraint( GraphDatabaseService db, Transaction tx, Label label, String property )
    {
        createUniquenessConstraint( db, tx, label.name(), property );
    }

    public abstract void createUniquenessConstraint( GraphDatabaseService db, Transaction tx, String label, String property );

    public void createNodeKeyConstraint( GraphDatabaseService db, Transaction tx, String label, String... properties )
    {
        createNodeKeyConstraint( db, tx, Label.label( label ), properties );
    }

    public abstract void createNodeKeyConstraint( GraphDatabaseService db, Transaction tx, Label label, String... properties );

    public void createNodePropertyExistenceConstraint( GraphDatabaseService db, Transaction tx, Label label, String property )
    {
        createNodePropertyExistenceConstraint( db, tx, label.name(), property );
    }

    public abstract void createNodePropertyExistenceConstraint( GraphDatabaseService db, Transaction tx, String label, String property );

    public void createRelPropertyExistenceConstraint( GraphDatabaseService db, Transaction tx, RelationshipType type, String property )
    {
        createRelPropertyExistenceConstraint( db, tx, type.name(), property );
    }

    public abstract void createRelPropertyExistenceConstraint( GraphDatabaseService db, Transaction tx, String type, String property );

    public void awaitIndexes( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.commit();
        }
    }

    public abstract ConstraintDefinition createNodeKeyConstraint( Transaction tx, String name, Label label, String... propertyKey );
}
