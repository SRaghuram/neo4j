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
package org.neo4j.concurrencytest;

import org.junit.Rule;
import org.junit.Test;

import java.util.function.Supplier;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.test.rule.concurrent.ThreadingRule;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;

public class ConstraintIndexConcurrencyTest
{
    @Rule
    public final DbmsRule db = new ImpermanentDbmsRule();
    @Rule
    public final ThreadingRule threads = new ThreadingRule();

    @Test
    public void shouldNotAllowConcurrentViolationOfConstraint() throws Exception
    {
        // Given
        GraphDatabaseAPI graphDb = db.getGraphDatabaseAPI();

        Supplier<KernelTransaction> ktxSupplier = () -> graphDb.getDependencyResolver()
                .resolveDependency( ThreadToStatementContextBridge.class ).getKernelTransactionBoundToThisThread( true, db.databaseId() );

        Label label = label( "Foo" );
        String propertyKey = "bar";
        String conflictingValue = "baz";

        // a constraint
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).create();
            tx.commit();
        }

        // When
        try ( Transaction tx = graphDb.beginTx() )
        {
            KernelTransaction ktx = ktxSupplier.get();
            int labelId = ktx.tokenRead().nodeLabel( label.name() );
            int propertyKeyId = ktx.tokenRead().propertyKey( propertyKey );
            Read read = ktx.dataRead();
            try ( NodeValueIndexCursor cursor = ktx.cursors().allocateNodeValueIndexCursor() )
            {
                IndexReadSession index = ktx.dataRead().indexReadSession( ktx.schemaRead().index( labelId, propertyKeyId ) );
                read.nodeIndexSeek( index, cursor, IndexOrder.NONE, false,
                        IndexQuery.exact( propertyKeyId,
                                "The value is irrelevant, we just want to perform some sort of lookup against this " + "index" ) );
            }
            // then let another thread come in and create a node
            threads.execute( db ->
            {
                try ( Transaction transaction = db.beginTx() )
                {
                    db.createNode( label ).setProperty( propertyKey, conflictingValue );
                    transaction.commit();
                }
                return null;
            }, graphDb ).get();

            // before we create a node with the same property ourselves - using the same statement that we have
            // already used for lookup against that very same index
            long node = ktx.dataWrite().nodeCreate();
            ktx.dataWrite().nodeAddLabel( node, labelId );
            try
            {
                ktx.dataWrite().nodeSetProperty( node, propertyKeyId, Values.of( conflictingValue ) );

                fail( "exception expected" );
            }
            // Then
            catch ( UniquePropertyValueValidationException e )
            {
                assertEquals( ConstraintDescriptorFactory.uniqueForLabel( labelId, propertyKeyId ), e.constraint() );
                IndexEntryConflictException conflict = Iterators.single( e.conflicts().iterator() );
                assertEquals( Values.stringValue( conflictingValue ), conflict.getSinglePropertyValue() );
            }

            tx.commit();
        }
    }
}
