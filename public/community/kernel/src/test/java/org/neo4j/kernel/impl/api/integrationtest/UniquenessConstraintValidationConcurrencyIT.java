/*
 * Copyright (c) 2002-2019 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.function.Function;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.test.rule.concurrent.OtherThreadRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.test.rule.concurrent.OtherThreadRule.isWaiting;

public class UniquenessConstraintValidationConcurrencyIT
{
    @Rule
    public final DatabaseRule database = new ImpermanentDatabaseRule();
    @Rule
    public final OtherThreadRule<Void> otherThread = new OtherThreadRule<>();

    @Test
    public void shouldAllowConcurrentCreationOfNonConflictingData() throws Exception
    {
        // given
        database.executeAndCommit( createUniquenessConstraint( "Label1", "key1" ) );

        // when
        Future<Boolean> created = database.executeAndCommit( db -> {
            db.createNode( label( "Label1" ) ).setProperty( "key1", "value1" );
            return otherThread.execute( createNode( db, "Label1", "key1", "value2" ) );
        } );

        // then
        assertTrue( "Node creation should succeed", created.get() );
    }

    @Test
    public void shouldPreventConcurrentCreationOfConflictingData() throws Exception
    {
        // given
        database.executeAndCommit( createUniquenessConstraint( "Label1", "key1" ) );

        // when
        Future<Boolean> created = database.executeAndCommit( db -> {
            db.createNode( label( "Label1" ) ).setProperty( "key1", "value1" );
            try
            {
                return otherThread.execute( createNode( db, "Label1", "key1", "value1" ) );
            }
            finally
            {
                assertThat( otherThread, isWaiting() );
            }
        } );

        // then
        assertFalse( "node creation should fail", created.get() );
    }

    @Test
    public void shouldAllowOtherTransactionToCompleteIfFirstTransactionRollsBack() throws Exception
    {
        // given
        database.executeAndCommit( createUniquenessConstraint( "Label1", "key1" ) );

        // when
        Future<Boolean> created = database.executeAndRollback( db -> {
            db.createNode( label( "Label1" ) ).setProperty( "key1", "value1" );
            try
            {
                return otherThread.execute( createNode( db, "Label1", "key1", "value1" ) );
            }
            finally
            {
                assertThat( otherThread, isWaiting() );
            }
        } );

        // then
        assertTrue( "Node creation should succeed", created.get() );
    }

    private static Function<GraphDatabaseService, Void> createUniquenessConstraint(
            final String label, final String propertyKey )
    {
        return db -> {
            db.schema().constraintFor( label( label ) ).assertPropertyIsUnique( propertyKey ).create();
            return null;
        };
    }

    public static OtherThreadExecutor.WorkerCommand<Void, Boolean> createNode(
            final GraphDatabaseService db, final String label, final String propertyKey, final Object propertyValue )
    {
        return nothing -> {
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode( label( label ) ).setProperty( propertyKey, propertyValue );

                tx.success();
                return true;
            }
            catch ( ConstraintViolationException e )
            {
                return false;
            }
        };
    }
}
