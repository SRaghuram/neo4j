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
                public void createIndex( GraphDatabaseService db, String label, String property )
                {
                    db.execute( format( "CREATE INDEX ON :`%s`(`%s`)", label, property ) );
                }

                @Override
                public void createUniquenessConstraint( GraphDatabaseService db, String label, String property )
                {
                    db.execute( format( "CREATE CONSTRAINT ON (n:`%s`) ASSERT n.`%s` IS UNIQUE", label, property ) );
                }

                @Override
                public void createNodeKeyConstraint( GraphDatabaseService db, Label label, String... properties )
                {
                    String keyProperties = Arrays.stream( properties )
                            .map( property -> format("n.`%s`", property))
                            .collect( joining( "," ) );
                    db.execute( format( "CREATE CONSTRAINT ON (n:`%s`) ASSERT (%s) IS NODE KEY", label.name(), keyProperties ) );
                }

                @Override
                public void createNodePropertyExistenceConstraint( GraphDatabaseService db, String label, String property )
                {
                    db.execute( format( "CREATE CONSTRAINT ON (n:`%s`) ASSERT exists(n.`%s`)", label, property ) );
                }

                @Override
                public void createRelPropertyExistenceConstraint( GraphDatabaseService db, String type, String property )
                {
                    db.execute( format( "CREATE CONSTRAINT ON ()-[r:`%s`]-() ASSERT exists(r.`%s`)", type, property ) );
                }

                @Override
                public ConstraintDefinition createNodeKeyConstraint( GraphDatabaseService db, String name, Label label, String... propertyKey )
                {
                    throw new TestAbortedException( "Cypher cannot yet create NAMED node key constraints." );
                }
            },
    CORE_API
            {
                @Override
                public void createIndex( GraphDatabaseService db, String label, String property )
                {
                    db.schema().indexFor( Label.label( label ) ).on( property ).create();
                }

                @Override
                public void createUniquenessConstraint( GraphDatabaseService db, String label, String property )
                {
                    db.schema().constraintFor( Label.label( label ) ).assertPropertyIsUnique( property ).create();
                }

                @Override
                public void createNodeKeyConstraint( GraphDatabaseService db, Label label, String... properties )
                {
                    ConstraintCreator creator = db.schema().constraintFor( label );
                    for ( String property : properties )
                    {
                        creator = creator.assertPropertyIsNodeKey( property );
                    }
                    creator.create();
                }

                @Override
                public void createNodePropertyExistenceConstraint( GraphDatabaseService db, String label, String property )
                {
                    db.schema().constraintFor( Label.label( label ) ).assertPropertyExists( property ).create();
                }

                @Override
                public void createRelPropertyExistenceConstraint( GraphDatabaseService db, String type, String property )
                {
                    db.schema().constraintFor( RelationshipType.withName( type ) ).assertPropertyExists( property ).create();
                }

                @Override
                public ConstraintDefinition createNodeKeyConstraint( GraphDatabaseService db, String name, Label label, String... keys )
                {
                    ConstraintCreator creator = db.schema().constraintFor( label ).withName( name );
                    for ( String key : keys )
                    {
                        creator = creator.assertPropertyIsNodeKey( key );
                    }
                    return creator.create();
                }
            };

    public abstract void createIndex( GraphDatabaseService db, String label, String property );

    public void createUniquenessConstraint( GraphDatabaseService db, Label label, String property )
    {
        createUniquenessConstraint( db, label.name(), property );
    }

    public abstract void createUniquenessConstraint( GraphDatabaseService db, String label, String property );

    public void createNodeKeyConstraint( GraphDatabaseService db, String label, String... properties )
    {
        createNodeKeyConstraint( db, Label.label( label ), properties );
    }

    public abstract void createNodeKeyConstraint( GraphDatabaseService db, Label label, String... properties );

    public void createNodePropertyExistenceConstraint( GraphDatabaseService db, Label label, String property )
    {
        createNodePropertyExistenceConstraint( db, label.name(), property );
    }

    public abstract void createNodePropertyExistenceConstraint( GraphDatabaseService db, String label, String property );

    public void createRelPropertyExistenceConstraint( GraphDatabaseService db, RelationshipType type, String property )
    {
        createRelPropertyExistenceConstraint( db, type.name(), property );
    }

    public abstract void createRelPropertyExistenceConstraint( GraphDatabaseService db, String type, String property );

    public void awaitIndexes( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.commit();
        }
    }

    public abstract ConstraintDefinition createNodeKeyConstraint( GraphDatabaseService db, String name, Label label, String... propertyKey );
}
