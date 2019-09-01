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
package org.neo4j.kernel.impl.api;

import org.junit.Rule;
import org.junit.Test;

import java.util.function.Function;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;

public class DataAndSchemaTransactionSeparationIT
{
    @Rule
    public final DbmsRule db = new ImpermanentDbmsRule();

    private Function<Transaction, Void> expectFailureAfterSchemaOperation(
            final Function<Transaction, ?> function )
    {
        return graphDb ->
        {
            // given
            db.getGraphDatabaseAPI().schema().indexFor( label( "Label1" ) ).on( "key1" ).create();

            // when
            try
            {
                function.apply( graphDb );

                fail( "expected exception" );
            }
            // then
            catch ( Exception e )
            {
                assertEquals( "Cannot perform data updates in a transaction that has performed schema updates.",
                        e.getMessage() );
            }
            return null;
        };
    }

    private Function<Transaction, Void> succeedAfterSchemaOperation(
            final Function<Transaction, ?> function )
    {
        return graphDb ->
        {
            // given
            db.getGraphDatabaseAPI().schema().indexFor( label( "Label1" ) ).on( "key1" ).create();

            // when/then
            function.apply( graphDb );
            return null;
        };
    }

    @Test
    public void shouldNotAllowNodeCreationInSchemaTransaction()
    {
        db.executeAndRollback( expectFailureAfterSchemaOperation( createNode() ) );
    }

    @Test
    public void shouldNotAllowRelationshipCreationInSchemaTransaction()
    {
        // given
        final Pair<Node, Node> nodes = db.executeAndCommit( aPairOfNodes() );
        // then
        db.executeAndRollback( expectFailureAfterSchemaOperation( relate( nodes ) ) );
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void shouldNotAllowPropertyWritesInSchemaTransaction()
    {
        // given
        Pair<Node, Node> nodes = db.executeAndCommit( aPairOfNodes() );
        Relationship relationship = db.executeAndCommit( relate( nodes ) );
        // when
        for ( Function<Transaction, ?> operation : new Function[]{
                propertyWrite( Node.class, nodes.first(), "key1", "value1" ),
                propertyWrite( Relationship.class, relationship, "key1", "value1" ),
        } )
        {
            // then
            db.executeAndRollback( expectFailureAfterSchemaOperation( operation ) );
        }
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void shouldAllowPropertyReadsInSchemaTransaction()
    {
        // given
        Pair<Node, Node> nodes = db.executeAndCommit( aPairOfNodes() );
        Relationship relationship = db.executeAndCommit( relate( nodes ) );
        db.executeAndCommit( propertyWrite( Node.class, nodes.first(), "key1", "value1" ) );
        db.executeAndCommit( propertyWrite( Relationship.class, relationship, "key1", "value1" ) );

        // when
        for ( Function<Transaction, ?> operation : new Function[]{
                propertyRead( Node.class, nodes.first(), "key1" ),
                propertyRead( Relationship.class, relationship, "key1" ),
        } )
        {
            // then
            db.executeAndRollback( succeedAfterSchemaOperation( operation ) );
        }
    }

    private static Function<Transaction, Node> createNode()
    {
        return Transaction::createNode;
    }

    private static <T extends PropertyContainer> Function<Transaction, Object> propertyRead(
            Class<T> type, final T entity, final String key )
    {
        return new FailureRewrite<>( type.getSimpleName() + ".getProperty()" )
        {
            @Override
            Object perform( Transaction transaction )
            {
                return entity.getProperty( key );
            }
        };
    }

    private static <T extends PropertyContainer> Function<Transaction, Void> propertyWrite(
            Class<T> type, final T entity, final String key, final Object value )
    {
        return new FailureRewrite<Void>( type.getSimpleName() + ".setProperty()" )
        {
            @Override
            Void perform( Transaction transaction )
            {
                entity.setProperty( key, value );
                return null;
            }
        };
    }

    private static Function<Transaction, Pair<Node, Node>> aPairOfNodes()
    {
        return tx -> Pair.of( tx.createNode(), tx.createNode() );
    }

    private static Function<Transaction, Relationship> relate( final Pair<Node, Node> nodes )
    {
        return graphDb -> nodes.first().createRelationshipTo( nodes.other(), withName( "RELATED" ) );
    }

    private abstract static class FailureRewrite<T> implements Function<Transaction, T>
    {
        private final String message;

        FailureRewrite( String message )
        {
            this.message = message;
        }

        @Override
        public T apply( Transaction transaction )
        {
            try
            {
                return perform( transaction );
            }
            catch ( AssertionError e )
            {
                throw new AssertionError( message + ": " + e.getMessage(), e );
            }
        }

        abstract T perform( Transaction transaction );
    }
}
