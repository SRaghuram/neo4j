/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.core;

import org.junit.ClassRule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasProperty;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.inTx;

public class TestShortStringProperties
{
    private static final String LONG_STRING = "this is a really long string, believe me!";
    @ClassRule
    public static DbmsRule graphdb = new ImpermanentDbmsRule();

    @Test
    public void canAddMultipleShortStringsToTheSameNode()
    {
        Node node;
        try ( Transaction transaction = graphdb.beginTx() )
        {
            node = graphdb.getGraphDatabaseAPI().createNode();
            node.setProperty( "key", "value" );
            node.setProperty( "reverse", "esrever" );
            transaction.success();
        }
        assertThat( node, inTx( graphdb.getGraphDatabaseAPI(), hasProperty( "key" ).withValue( "value" )  ) );
        assertThat( node, inTx( graphdb.getGraphDatabaseAPI(), hasProperty( "reverse" ).withValue( "esrever" )  ) );
    }

    @Test
    public void canAddShortStringToRelationship()
    {
        Relationship rel;
        GraphDatabaseService db = graphdb.getGraphDatabaseAPI();
        try ( Transaction transaction = graphdb.beginTx() )
        {
            rel = db.createNode().createRelationshipTo( db.createNode(), withName( "REL_TYPE" ) );
            rel.setProperty( "type", "dimsedut" );
            transaction.success();
        }
        assertThat( rel, inTx( db, hasProperty( "type" ).withValue( "dimsedut" ) ) );
    }

    @Test
    public void canUpdateShortStringInplace()
    {
        Node node;
        try ( Transaction transaction = graphdb.beginTx() )
        {
            node = graphdb.getGraphDatabaseAPI().createNode();
            node.setProperty( "key", "value" );
            transaction.success();
        }

        try ( Transaction transaction = graphdb.beginTx() )
        {
            assertEquals( "value", node.getProperty( "key" ) );
            node.setProperty( "key", "other" );
            transaction.success();
        }

        assertThat( node, inTx( graphdb.getGraphDatabaseAPI(), hasProperty( "key" ).withValue( "other" )  ) );
    }

    @Test
    public void canReplaceLongStringWithShortString()
    {
        Node node;
        try ( Transaction transaction = graphdb.beginTx() )
        {
            node = graphdb.getGraphDatabaseAPI().createNode();
            node.setProperty( "key", LONG_STRING );
            transaction.success();
        }

        try ( Transaction transaction = graphdb.beginTx() )
        {
            assertEquals( LONG_STRING, node.getProperty( "key" ) );
            node.setProperty( "key", "value" );
            transaction.success();
        }

        assertThat( node, inTx( graphdb.getGraphDatabaseAPI(), hasProperty( "key" ).withValue( "value" )  ) );
    }

    @Test
    public void canReplaceShortStringWithLongString()
    {
        Node node;
        try ( Transaction transaction = graphdb.beginTx() )
        {
            node = graphdb.getGraphDatabaseAPI().createNode();
            node.setProperty( "key", "value" );
            transaction.success();
        }

        try ( Transaction transaction = graphdb.beginTx() )
        {
            assertEquals( "value", node.getProperty( "key" ) );
            node.setProperty( "key", LONG_STRING );
            transaction.success();
        }

        assertThat( node, inTx( graphdb.getGraphDatabaseAPI(), hasProperty( "key" ).withValue( LONG_STRING )  ) );
    }

    @Test
    public void canRemoveShortStringProperty()
    {
        GraphDatabaseService db = graphdb.getGraphDatabaseAPI();
        Node node;
        try ( Transaction transaction = graphdb.beginTx() )
        {
            node = db.createNode();
            node.setProperty( "key", "value" );
            transaction.success();
        }

        try ( Transaction transaction = graphdb.beginTx() )
        {
            assertEquals( "value", node.getProperty( "key" ) );

            node.removeProperty( "key" );
            transaction.success();
        }

        assertThat( node, inTx( db, not( hasProperty( "key" ) ) ) );
    }
}
