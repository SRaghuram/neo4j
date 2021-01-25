/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import com.ldbc.driver.DbException;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.util.Tuple2;
import com.neo4j.bench.ldbc.Domain;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

import static java.lang.String.format;

public class LdbcIndexer
{
    private static final Logger LOGGER = Logger.getLogger( LdbcIndexer.class );

    private final Neo4jSchema neo4jSchema;
    private final boolean withUnique;
    private final boolean withMandatory;
    private final boolean dropFirst;

    public LdbcIndexer(
            Neo4jSchema neo4jSchema,
            boolean withUnique,
            boolean withMandatory )
    {
        this( neo4jSchema, withUnique, withMandatory, false );
    }

    public LdbcIndexer(
            Neo4jSchema neo4jSchema,
            boolean withUnique,
            boolean withMandatory,
            boolean dropFirst )
    {
        this.neo4jSchema = neo4jSchema;
        this.withUnique = withUnique;
        this.withMandatory = withMandatory;
        this.dropFirst = dropFirst;
    }

    public void createTransactional( GraphDatabaseService db ) throws DbException
    {
        if ( dropFirst )
        {
            dropSchema( db );
        }
        Set<Tuple2<Label,String>> nodesToMandatory =
                withMandatory ? Domain.nodesToMandatory( neo4jSchema ) : new HashSet<>();
        Set<Tuple2<RelationshipType,String>> relationshipsToMandatory =
                withMandatory ? Domain.relationshipsToMandatory( neo4jSchema ) : new HashSet<>();
        Set<Tuple2<Label,String>> toUnique = withUnique ? Domain.toUnique( neo4jSchema ) : new HashSet<>();
        Set<Tuple2<Label,String>> toIndex = Domain.toIndex( neo4jSchema, toUnique );
        createMandatoryConstraints( db, nodesToMandatory, relationshipsToMandatory );
        createUniqueConstraints( db, toUnique );
        createSchemaIndexes( db, toIndex );
        waitForIndexesToBeOnline( db );
    }

    private static void dropSchema( GraphDatabaseService db )
    {
        LOGGER.info( "Dropping indexes and constraints..." );
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().getConstraints().forEach( ConstraintDefinition::drop );
            tx.schema().getIndexes().forEach( IndexDefinition::drop );
            tx.commit();
        }
    }

    private static void createSchemaIndexes(
            GraphDatabaseService db,
            Iterable<Tuple2<Label,String>> labelPropertyPairsToIndexCreate )
    {
        if ( labelPropertyPairsToIndexCreate.iterator().hasNext() )
        {
            LOGGER.info( "Creating Schema Indexes For" );
            for ( Tuple2<Label,String> schemaIndex : labelPropertyPairsToIndexCreate )
            {
                Label label = schemaIndex._1();
                String property = schemaIndex._2();
                LOGGER.info( format( "\t(%s , %s)", label, property ) );
                String query = "CREATE INDEX ON :" + label + "(" + property + ")";
                try ( Transaction tx = db.beginTx() )
                {
                    tx.execute( query );
                    tx.commit();
                }
                catch ( Exception createException )
                {
                    LOGGER.error( format(
                            "\t\tFailed\n%s",
                            ConcurrentErrorReporter.stackTraceToString( createException ) ) );
                }
            }
        }
        else
        {
            LOGGER.info( "No Schema Indexes To Create" );
        }
    }

    private static void createUniqueConstraints(
            GraphDatabaseService db,
            Iterable<Tuple2<Label,String>> labelPropertyPairsToConstraintCreate )
    {
        if ( labelPropertyPairsToConstraintCreate.iterator().hasNext() )
        {
            LOGGER.info( "Creating Unique Constraints For" );
            for ( Tuple2<Label,String> constraintIndex : labelPropertyPairsToConstraintCreate )
            {
                Label label = constraintIndex._1();
                String property = constraintIndex._2();
                LOGGER.info( format( "\t(%s , %s)", label, property ) );
                String query = "CREATE CONSTRAINT ON (node:" + label + ") ASSERT node." + property + " IS UNIQUE";
                try ( Transaction tx = db.beginTx() )
                {
                    tx.execute( query );
                    tx.commit();
                }
                catch ( Exception createException )
                {
                    LOGGER.error( format(
                            "\t\tFailed\n%s",
                            ConcurrentErrorReporter.stackTraceToString( createException ) ) );
                }
            }
        }
        else
        {
            LOGGER.info( "No Unique Constraints To Create" );
        }
    }

    private static void createMandatoryConstraints(
            GraphDatabaseService db,
            Iterable<Tuple2<Label,String>> nodesToMandatory,
            Iterable<Tuple2<RelationshipType,String>> relationshipsToMandatory )
    {
        if ( nodesToMandatory.iterator().hasNext() )
        {
            LOGGER.info( "Creating Node Mandatory Constraints For" );
            for ( Tuple2<Label,String> schemaIndex : nodesToMandatory )
            {
                Label label = schemaIndex._1();
                String property = schemaIndex._2();
                LOGGER.info( format( "\t(%s , %s)", label, property ) );
                try ( Transaction tx = db.beginTx() )
                {
                    tx.execute(
                            "CREATE CONSTRAINT ON (node:" + label.name() + ") ASSERT exists(node." + property + ")" );
                    tx.commit();
                }
                catch ( Exception createException )
                {
                    LOGGER.error( format(
                            "\t\tFailed\n%s",
                            ConcurrentErrorReporter.stackTraceToString( createException ) ) );
                }
            }
        }
        else
        {
            LOGGER.info( "No Node Mandatory Constraints To Create" );
        }
        if ( relationshipsToMandatory.iterator().hasNext() )
        {
            LOGGER.info( "Creating Relationship Mandatory Constraints For" );
            for ( Tuple2<RelationshipType,String> schemaIndex : relationshipsToMandatory )
            {
                RelationshipType type = schemaIndex._1();
                String property = schemaIndex._2();
                LOGGER.info( format( "\t(%s , %s)", type, property ) );
                try ( Transaction tx = db.beginTx() )
                {
                    tx.execute( "CREATE CONSTRAINT ON (rel:" + type.name() + ") ASSERT exists(rel." + property + ")" );
                    tx.commit();
                }
                catch ( Exception createException )
                {
                    LOGGER.error( format(
                            "\t\tFailed\n%s",
                            ConcurrentErrorReporter.stackTraceToString( createException ) ) );
                }
            }
        }
        else
        {
            LOGGER.info( "No Relationship Mandatory Constraints To Create" );
        }
    }

    public static void waitForIndexesToBeOnline( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            var schema = tx.schema();
            if ( schema.getIndexes().iterator().hasNext() )
            {
                LOGGER.info( "Waiting For Indexes To Build" );
            }
            for ( IndexDefinition def : schema.getIndexes() )
            {
                LOGGER.info( format( "\t(%s , %s) - %s",
                        def.getLabels(), def.getPropertyKeys(), schema.getIndexState( def ) ) );

                assertIndexNotFailed( tx, def );

                if ( schema.getIndexState( def ) != Schema.IndexState.ONLINE )
                {
                    while ( schema.getIndexState( def ) == Schema.IndexState.POPULATING )
                    {
                        Thread.sleep( 500 );
                    }
                    LOGGER.info( format( "\t(%s , %s) - %s",
                            def.getLabels(), def.getPropertyKeys(), schema.getIndexState( def ) ) );

                    assertIndexNotFailed( tx, def );
                }
            }
            tx.commit();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error while waiting for indexes to come online", e );
        }
    }

    private static void assertIndexNotFailed( Transaction tx, IndexDefinition def )
    {
        if ( tx.schema().getIndexState( def ) == Schema.IndexState.FAILED )
        {
            throw new RuntimeException( format( "Index (%s,%s) failed to build:\n%s",
                    def.getLabels(),
                    def.getPropertyKeys(),
                    tx.schema().getIndexFailure( def ) ) );
        }
    }
}
