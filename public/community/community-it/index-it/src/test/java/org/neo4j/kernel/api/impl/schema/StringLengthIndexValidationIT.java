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
package org.neo4j.kernel.api.impl.schema;

import org.apache.commons.lang3.RandomStringUtils;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.EmbeddedDbmsRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.TestLabels.LABEL_ONE;

public abstract class StringLengthIndexValidationIT
{
    private static final String propKey = "largeString";
    private int singleKeySizeLimit = getSingleKeySizeLimit();
    private GraphDatabaseSettings.SchemaIndex schemaIndex = getSchemaIndex();

    @Rule
    public DbmsRule db = new EmbeddedDbmsRule()
            .withSetting( GraphDatabaseSettings.default_schema_provider, schemaIndex.providerName() );

    protected abstract int getSingleKeySizeLimit();

    protected abstract GraphDatabaseSettings.SchemaIndex getSchemaIndex();

    protected abstract String expectedPopulationFailureMessage();

    @Test
    public void shouldSuccessfullyWriteAndReadWithinIndexKeySizeLimit()
    {
        createIndex();
        String propValue = getString( singleKeySizeLimit );
        long expectedNodeId;

        // Write
        expectedNodeId = createNode( propValue );

        // Read
        assertReadNode( propValue, expectedNodeId );
    }

    @Test
    public void shouldSuccessfullyPopulateIndexWithinIndexKeySizeLimit()
    {
        String propValue = getString( singleKeySizeLimit );
        long expectedNodeId;

        // Write
        expectedNodeId = createNode( propValue );

        // Populate
        createIndex();

        // Read
        assertReadNode( propValue, expectedNodeId );
    }

    @Test
    public void txMustFailIfExceedingIndexKeySizeLimit()
    {
        createIndex();

        // Write
        try ( Transaction tx = db.beginTx() )
        {
            String propValue = getString( singleKeySizeLimit + 1 );
            db.createNode( LABEL_ONE ).setProperty( propKey, propValue );
            tx.success();
        }
        catch ( IllegalArgumentException e )
        {
            assertThat( e.getMessage(), Matchers.containsString(
                    "Property value is too large to index into this particular index. Please see index documentation for limitations." ) );
        }
    }

    @Test
    public void indexPopulationMustFailIfExceedingIndexKeySizeLimit()
    {
        // Write
        String propValue = getString( singleKeySizeLimit + 1 );
        createNode( propValue );

        // Create index should be fine
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( LABEL_ONE ).on( propKey ).create();
            tx.success();
        }

        // Waiting for it to come online should fail
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
        catch ( IllegalStateException e )
        {
            GraphDatabaseSettings.SchemaIndex schemaIndex = getSchemaIndex();
            assertThat( e.getMessage(), Matchers.containsString(
                    String.format(
                            "Index IndexDefinition[label:LABEL_ONE on:largeString] (IndexRule[id=1, descriptor=Index( GENERAL, :label[0](property[0]) ), " +
                                    "provider={key=%s, version=%s}]) entered a FAILED state.",
                            schemaIndex.providerKey(), schemaIndex.providerVersion() ) ) );
        }

        // Index should be in failed state
        try ( Transaction tx = db.beginTx() )
        {
            Iterator<IndexDefinition> iterator = db.schema().getIndexes( LABEL_ONE ).iterator();
            assertTrue( iterator.hasNext() );
            IndexDefinition next = iterator.next();
            assertEquals( "state is FAILED", Schema.IndexState.FAILED, db.schema().getIndexState( next ) );
            assertThat( db.schema().getIndexFailure( next ),
                    Matchers.containsString( expectedPopulationFailureMessage() ) );
            tx.success();
        }
    }

    // Each char in string need to fit in one byte
    private String getString( int byteArraySize )
    {
        return RandomStringUtils.randomAlphabetic( byteArraySize );
    }

    private void createIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( LABEL_ONE ).on( propKey ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
    }

    private long createNode( String propValue )
    {
        long expectedNodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( LABEL_ONE );
            node.setProperty( propKey, propValue );
            expectedNodeId = node.getId();
            tx.success();
        }
        return expectedNodeId;
    }

    private void assertReadNode( String propValue, long expectedNodeId )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.findNode( LABEL_ONE, propKey, propValue );
            assertNotNull( node );
            assertEquals( "node id", expectedNodeId, node.getId() );
            tx.success();
        }
    }
}
