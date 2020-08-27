/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.storeupgrade;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@Neo4jLayoutExtension
@ExtendWith( RandomExtension.class )
class StandardToHighLimitMigrationIT
{
    private static final Label[] labels = new Label[]{
            Label.label( "label1" ),
            Label.label( "label2" ),
            Label.label( "label3" ),
            Label.label( "label4" ),
            Label.label( "label5" )
    };
    private static final RelationshipType[] relTypes = new RelationshipType[]{
            RelationshipType.withName( "relType1" ),
            RelationshipType.withName( "relType2" ),
            RelationshipType.withName( "relType3" ),
            RelationshipType.withName( "relType4" ),
            RelationshipType.withName( "relType5" )
    };
    private static final String[] propKeys = new String[]{
            "prop1",
            "prop2",
            "prop3",
            "prop4",
            "prop5"
    };
    private final List<Long> nodes = new ArrayList<>();
    @Inject
    private Neo4jLayout neo4jLayout;
    @Inject
    private RandomRule randomRule;

    @ParameterizedTest
    @CsvSource( {"standard,high_limit", "standard,aligned", "aligned,high_limit"} )
    void shouldUpgradeFromStandardToHighLimitFormat( String fromFormat, String toFormat )
    {
        DbRepresentation expected;

        // Create standard store
        DatabaseManagementService dbms = new TestDatabaseManagementServiceBuilder( neo4jLayout )
                .setConfig( GraphDatabaseSettings.record_format, fromFormat )
                .build();
        try
        {
            GraphDatabaseService db = dbms.database( DEFAULT_DATABASE_NAME );

            int nbrOfOperations = 10;
            for ( int i = 0; i < nbrOfOperations; i++ )
            {
                randomOperation( db );
            }
            expected = DbRepresentation.of( db );
        }
        finally
        {
            dbms.shutdown();
        }

        dbms = new TestDatabaseManagementServiceBuilder( neo4jLayout )
                .setConfig( GraphDatabaseSettings.allow_upgrade, true )
                .setConfig( GraphDatabaseSettings.record_format, toFormat )
                .build();

        try
        {
            GraphDatabaseService db = dbms.database( DEFAULT_DATABASE_NAME );
            DbRepresentation actual = DbRepresentation.of( db );
            Collection<String> diff = actual.compareWith( expected );
            assertTrue( diff.isEmpty(), "Expected store to have the same content after upgrade but it differed: " + diff );
        }
        finally
        {
            dbms.shutdown();
        }
    }

    private void randomOperation( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            double action = randomRule.nextDouble();
            if ( action < 0.025 )
            {
                createIndex( tx );
            }
            else if ( action < 0.05 )
            {
                createConstraint( tx );
            }
            else if ( action < 0.40 )
            {
                createNode( tx );
            }
            else
            {
                createRelationship( tx );
            }
            tx.commit();
        }
    }

    private void createNode( Transaction tx )
    {
        Node node = tx.createNode();
        for ( Label label : randomRule.selection( labels, 0, labels.length, false ) )
        {
            node.addLabel( label );
        }
        addProperties( node );
        nodes.add( node.getId() );
    }

    private void createRelationship( Transaction tx )
    {
        if ( nodes.size() == 0 )
        {
            return;
        }

        Node from = tx.getNodeById( randomRule.among( nodes ) );
        Node to = tx.getNodeById( randomRule.among( nodes ) );
        Relationship relationship = from.createRelationshipTo( to, randomRule.among( relTypes ) );
        addProperties( relationship );
    }

    private void addProperties( Entity entity )
    {
        for ( String key : randomRule.selection( propKeys, 0, propKeys.length, false ) )
        {
            entity.setProperty( key, randomRule.nextValueAsObject() );
        }
    }

    private void createIndex( Transaction tx )
    {
        String indexName = randomRule.nextAlphaNumericString();
        IndexType indexType = randomRule.among( IndexType.values() );
        String[] indexProperties = randomRule.selection( propKeys, 1, propKeys.length, false );
        Label indexLabel = randomRule.among( labels );

        IndexCreator indexCreator = tx.schema().indexFor( indexLabel )
                .withName( indexName )
                .withIndexType( indexType );
        for ( String prop : indexProperties )
        {
            indexCreator = indexCreator.on( prop );
        }

        try
        {
            indexCreator.create();
        }
        catch ( ConstraintViolationException e )
        {
            // Don't create it then
        }
    }

    private void createConstraint( Transaction tx )
    {
        ConstraintType type = randomRule.among( ConstraintType.values() );
        ConstraintCreator constraintCreator;
        switch ( type )
        {
        case UNIQUENESS:
        case NODE_PROPERTY_EXISTENCE:
        case NODE_KEY:
            constraintCreator = tx.schema().constraintFor( randomRule.among( labels ) );
            break;
        case RELATIONSHIP_PROPERTY_EXISTENCE:
            constraintCreator = tx.schema().constraintFor( randomRule.among( relTypes ) );
            break;
        default:
            throw new IllegalArgumentException( "Illegal type " + type );
        }

        switch ( type )
        {
        case UNIQUENESS:
            constraintCreator = constraintCreator.assertPropertyIsUnique( randomRule.among( propKeys ) );
            break;
        case NODE_KEY:
            constraintCreator = constraintCreator.assertPropertyIsNodeKey( randomRule.among( propKeys ) );
            break;
        case NODE_PROPERTY_EXISTENCE:
        case RELATIONSHIP_PROPERTY_EXISTENCE:
            constraintCreator = constraintCreator.assertPropertyExists( randomRule.among( propKeys ) );
            break;
        default:
            throw new IllegalArgumentException( "Illegal type " + type );
        }

        try
        {
            constraintCreator.create();
        }
        catch ( ConstraintViolationException e )
        {
            // Don't create it then
        }
    }
}
