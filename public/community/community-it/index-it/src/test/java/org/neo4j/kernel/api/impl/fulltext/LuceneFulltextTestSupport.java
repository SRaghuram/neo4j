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
package org.neo4j.kernel.api.impl.fulltext;

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipIndexCursor;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelImpl;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.coreapi.TopLevelTransaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.EmbeddedDbmsRule;
import org.neo4j.test.rule.RepeatRule;

import static java.lang.String.format;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LuceneFulltextTestSupport
{
    static final Label LABEL = Label.label( "LABEL" );
    static final RelationshipType RELTYPE = RelationshipType.withName( "type" );
    static final String PROP = "prop";

    DbmsRule db = new EmbeddedDbmsRule();
    private RepeatRule repeatRule = createRepeatRule();

    @Rule
    public RuleChain rules = RuleChain.outerRule( repeatRule ).around( db );

    IndexConfig indexConfig;
    FulltextAdapter fulltextAdapter;

    protected RepeatRule createRepeatRule()
    {
        return new RepeatRule( false, 1 );
    }

    @Before
    public void setUp()
    {
        indexConfig = IndexConfig.empty();
        fulltextAdapter = getAccessor();
    }

    void applySetting( Setting<String> setting, String value ) throws IOException
    {
        db.restartDatabase( setting.name(), value );
        db.ensureStarted();
        fulltextAdapter = getAccessor();
    }

    KernelTransactionImplementation getKernelTransaction()
    {
        try
        {
            return (KernelTransactionImplementation) db.resolveDependency( KernelImpl.class ).beginTransaction(
                    org.neo4j.internal.kernel.api.Transaction.Type.explicit, LoginContext.AUTH_DISABLED );
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException( "oops" );
        }
    }

    private FulltextAdapter getAccessor()
    {
        return (FulltextAdapter) db.resolveDependency( IndexProviderMap.class ).lookup( FulltextIndexProviderFactory.DESCRIPTOR );
    }

    long createNodeIndexableByPropertyValue( Label label, Object propertyValue )
    {
        return createNodeWithProperty( label, PROP, propertyValue );
    }

    long createNodeWithProperty( Label label, String propertyKey, Object propertyValue )
    {
        Node node = db.createNode( label );
        node.setProperty( propertyKey, propertyValue );
        return node.getId();
    }

    long createRelationshipIndexableByPropertyValue( long firstNodeId, long secondNodeId, Object propertyValue )
    {
        return createRelationshipWithProperty( firstNodeId, secondNodeId, PROP, propertyValue );
    }

    long createRelationshipWithProperty( long firstNodeId, long secondNodeId, String propertyKey, Object propertyValue )
    {
        Node first = db.getNodeById( firstNodeId );
        Node second = db.getNodeById( secondNodeId );
        Relationship relationship = first.createRelationshipTo( second, RELTYPE );
        relationship.setProperty( propertyKey, propertyValue );
        return relationship.getId();
    }

    public static KernelTransaction kernelTransaction( Transaction tx ) throws Exception
    {
        assertThat( tx, instanceOf( TopLevelTransaction.class ) );
        Field transactionField = TopLevelTransaction.class.getDeclaredField( "transaction" );
        transactionField.setAccessible( true );
        return (KernelTransaction) transactionField.get( tx );
    }

    void assertQueryFindsNothing( KernelTransaction ktx, boolean nodes, String indexName, String query ) throws Exception
    {
        assertQueryFindsIds( ktx, nodes, indexName, query );
    }

    void assertQueryFindsIds( KernelTransaction ktx, boolean nodes, String indexName, String query, long... ids ) throws Exception
    {
        IndexReference index = ktx.schemaRead().indexGetForName( indexName );
        IndexReadSession indexSession = ktx.dataRead().indexReadSession( index );
        MutableLongSet set = LongSets.mutable.of( ids );
        if ( nodes )
        {
            try ( NodeValueIndexCursor cursor = ktx.cursors().allocateNodeValueIndexCursor() )
            {
                ktx.dataRead().nodeIndexSeek( indexSession, cursor, IndexOrder.NONE, false, IndexQuery.fulltextSearch( query ) );
                while ( cursor.next() )
                {
                    long nodeId = cursor.nodeReference();
                    assertTrue( format( "Result returned node id %d, expected one of %s", nodeId, Arrays.toString( ids ) ), set.remove( nodeId ) );
                }
            }
        }
        else
        {
            try ( RelationshipIndexCursor cursor = ktx.cursors().allocateRelationshipIndexCursor() )
            {
                ktx.dataRead().relationshipIndexSeek( index, cursor, IndexQuery.fulltextSearch( query ) );
                while ( cursor.next() )
                {
                    long relationshipId = cursor.relationshipReference();
                    assertTrue( format( "Result returned relationship id %d, expected one of %s",
                            relationshipId, Arrays.toString( ids ) ), set.remove( relationshipId ) );
                }
            }
        }

        if ( !set.isEmpty() )
        {
            fail( "Number of results differ from expected. " + set.size() + " IDs were not found in the result: " + set );
        }
    }

    void assertQueryFindsNodeIdsInOrder( KernelTransaction ktx, String indexName, String query, long... ids )
            throws Exception
    {

        IndexReference index = ktx.schemaRead().indexGetForName( indexName );
        IndexReadSession indexSession = ktx.dataRead().indexReadSession( index );
        try ( NodeValueIndexCursor cursor = ktx.cursors().allocateNodeValueIndexCursor() )
        {
            int num = 0;
            float score = Float.MAX_VALUE;
            ktx.dataRead().nodeIndexSeek( indexSession, cursor, IndexOrder.NONE, false, IndexQuery.fulltextSearch( query ) );
            while ( cursor.next() )
            {
                long nextId = cursor.nodeReference();
                float nextScore = cursor.score();
                assertThat( nextScore, lessThanOrEqualTo( score ) );
                score = nextScore;
                assertEquals( format( "Result returned node id %d, expected %d", nextId, ids[num] ), ids[num], nextId );
                num++;
            }
            assertEquals( "Number of results differ from expected", ids.length, num );
        }
    }

    void setNodeProp( long nodeId, String value )
    {
        setNodeProp( nodeId, PROP, value );
    }

    void setNodeProp( long nodeId, String propertyKey, String value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.getNodeById( nodeId );
            node.setProperty( propertyKey, value );
            tx.success();
        }
    }

    void await( IndexReference descriptor ) throws Exception
    {
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            while ( tx.schemaRead().index( descriptor.schema() ) == IndexReference.NO_INDEX )
            {
                Thread.sleep( 100 );
            }
            while ( tx.schemaRead().indexGetState( descriptor ) != InternalIndexState.ONLINE )
            {
                Thread.sleep( 100 );
            }
        }
        catch ( InterruptedException e )
        {
            e.printStackTrace();
        }
    }
}
