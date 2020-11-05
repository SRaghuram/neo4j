/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintCreator;

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
                public void createNodeKeyConstraint( GraphDatabaseService db, Transaction tx, String label, String... properties )
                {
                    String keyProperties = Arrays.stream( properties )
                            .map( property -> format("n.`%s`", property))
                            .collect( joining( "," ) );
                    tx.execute( format( "CREATE CONSTRAINT ON (n:`%s`) ASSERT (%s) IS NODE KEY", label, keyProperties ) );
                }

                @Override
                public void createNodePropertyExistenceConstraint( GraphDatabaseService db, Transaction tx, String label, String property )
                {
                    tx.execute( format( "CREATE CONSTRAINT ON (n:`%s`) ASSERT (n.`%s`) IS NOT NULL", label, property ) );
                }

                @Override
                public void createRelPropertyExistenceConstraint( GraphDatabaseService db, Transaction tx, String type, String property )
                {
                    tx.execute( format( "CREATE CONSTRAINT ON ()-[r:`%s`]-() ASSERT (r.`%s`) IS NOT NULL", type, property ) );
                }

                @Override
                public void createNodeKeyConstraintWithName( Transaction tx, String name, String label,
                        String... propertyKey )
                {
                    String keyProperties = Arrays.stream( propertyKey )
                            .map( property -> format("n.`%s`", property))
                            .collect( joining( "," ) );
                    tx.execute( format( "CREATE CONSTRAINT `%s` ON (n:`%s`) ASSERT (%s) IS NODE KEY", name, label, keyProperties ) );
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
                public void createNodeKeyConstraint( GraphDatabaseService db, Transaction tx, String label, String... properties )
                {
                    ConstraintCreator creator = tx.schema().constraintFor( Label.label( label ) );
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
                public void createNodeKeyConstraintWithName( Transaction tx, String name, String label, String... keys )
                {
                    ConstraintCreator creator = tx.schema().constraintFor( Label.label( label ) ).withName( name );
                    for ( String key : keys )
                    {
                        creator = creator.assertPropertyIsNodeKey( key );
                    }
                    creator.create();
                }
            };

    /* --- Index --- */
    public abstract void createIndex( GraphDatabaseService db, Transaction tx, String label, String property );

    /* --- Uniqueness constraint --- */
    public void createUniquenessConstraint( GraphDatabaseService db, Transaction tx, Label label, String property )
    {
        createUniquenessConstraint( db, tx, label.name(), property );
    }

    public abstract void createUniquenessConstraint( GraphDatabaseService db, Transaction tx, String label, String property );

    /* --- NodeKey constraint --- */
    public void createNodeKeyConstraint( GraphDatabaseService db, Transaction tx, Label label, String... properties )
    {
        createNodeKeyConstraint( db, tx, label.name(), properties );
    }

    public void createNodeKeyConstraintWithName( Transaction tx, String name, Label label, String... propertyKey )
    {
        createNodeKeyConstraintWithName( tx, name, label.name(), propertyKey );
    }

    public abstract void createNodeKeyConstraint( GraphDatabaseService db, Transaction tx, String label, String... properties );

    public abstract void createNodeKeyConstraintWithName( Transaction tx, String name, String label, String... propertyKey );

    /* --- Node property existence constraint --- */
    public void createNodePropertyExistenceConstraint( GraphDatabaseService db, Transaction tx, Label label, String property )
    {
        createNodePropertyExistenceConstraint( db, tx, label.name(), property );
    }

    public abstract void createNodePropertyExistenceConstraint( GraphDatabaseService db, Transaction tx, String label, String property );

    /* --- Relationship property existence constraint --- */
    public void createRelPropertyExistenceConstraint( GraphDatabaseService db, Transaction tx, RelationshipType type, String property )
    {
        createRelPropertyExistenceConstraint( db, tx, type.name(), property );
    }

    public abstract void createRelPropertyExistenceConstraint( GraphDatabaseService db, Transaction tx, String type, String property );

    /* --- Util --- */
    public void awaitIndexes( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.commit();
        }
    }
}
