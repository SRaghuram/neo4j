/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package schema;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.index.IndexWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.index.internal.gbptree.TreeNodeDynamicSize.keyValueSizeCapFromPageSize;
import static org.neo4j.io.pagecache.PageCache.PAGE_SIZE;

@ExtendWith( TestDirectoryExtension.class )
class IndexValuesValidationTest
{
    @Inject
    private TestDirectory directory;

    private GraphDatabaseService database;
    private DatabaseManagementService managementService;

    void setUp( String... settings )
    {
        managementService = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( directory.storeDir() )
                .setConfig( stringMap( settings ) ).newDatabaseManagementService();
        database = managementService.database( DEFAULT_DATABASE_NAME );
    }

    @AfterEach
    void tearDown()
    {
        managementService.shutdown();
    }

    @Test
    void validateIndexedNodePropertiesInLucene()
    {
        setUp( default_schema_provider.name(), GraphDatabaseSettings.SchemaIndex.NATIVE30.providerName() );
        Label label = Label.label( "indexedNodePropertiesTestLabel" );
        String propertyName = "indexedNodePropertyName";

        createIndex( label, propertyName );

        try ( Transaction ignored = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 5, TimeUnit.MINUTES );
        }

        IllegalArgumentException argumentException = assertThrows( IllegalArgumentException.class, () ->
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node node = database.createNode( label );
                node.setProperty( propertyName, StringUtils.repeat( "a", IndexWriter.MAX_TERM_LENGTH + 1 ) );
                transaction.success();
            }
        } );
        assertThat( argumentException.getMessage(), equalTo( "Property value size is too large for index. Please see index documentation for limitations." ) );
    }

    @Test
    void validateIndexedNodePropertiesInNativeBtree()
    {
        setUp();
        Label label = Label.label( "indexedNodePropertiesTestLabel" );
        String propertyName = "indexedNodePropertyName";

        createIndex( label, propertyName );

        try ( Transaction ignored = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 5, TimeUnit.MINUTES );
        }

        IllegalArgumentException argumentException = assertThrows( IllegalArgumentException.class, () ->
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node node = database.createNode( label );
                node.setProperty( propertyName, StringUtils.repeat( "a", keyValueSizeCapFromPageSize( PAGE_SIZE ) + 1 ) );
                transaction.success();
            }
        } );
        assertThat( argumentException.getMessage(),
                containsString( "is too large to index into this particular index. Please see index documentation for limitations." ) );
    }

    @Test
    void validateNodePropertiesOnPopulation()
    {
        setUp();
        Label label = Label.label( "populationTestNodeLabel" );
        String propertyName = "populationTestPropertyName";

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            node.setProperty( propertyName, StringUtils.repeat( "a", IndexWriter.MAX_TERM_LENGTH + 1 ) );
            transaction.success();
        }

        IndexDefinition indexDefinition = createIndex( label, propertyName );
        try
        {
            try ( Transaction ignored = database.beginTx() )
            {
                database.schema().awaitIndexesOnline( 5, TimeUnit.MINUTES );
            }
        }
        catch ( IllegalStateException e )
        {
            try ( Transaction ignored = database.beginTx() )
            {
                String indexFailure = database.schema().getIndexFailure( indexDefinition );
                assertThat( indexFailure, allOf(
                        containsString( "java.lang.IllegalArgumentException:" ),
                        containsString( "Please see index documentation for limitations." )
                ) );
            }
        }
    }

    private IndexDefinition createIndex( Label label, String propertyName )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            IndexDefinition indexDefinition = database.schema().indexFor( label ).on( propertyName ).create();
            transaction.success();
            return indexDefinition;
        }
    }
}
