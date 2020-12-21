/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.newapi;

import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.impl.collector.Collectors2;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.values.storable.Values.stringValue;


abstract class IndexTransactionStateTestBase extends KernelAPIWriteTestBase<WriteTestSupport>
{
    static final String INDEX_NAME = "myIndex";
    static final String DEFAULT_PROPERTY_NAME = "prop";

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldPerformStringSuffixSearch( boolean needsValues ) throws Exception
    {
        // given
        Set<Pair<Long, Value>> expected = new HashSet<>();
        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( entityWithProp( tx, "1suff" ) );
            entityWithProp( tx, "pluff" );
            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            int prop = tx.tokenRead().propertyKey( DEFAULT_PROPERTY_NAME );
            expected.add( entityWithProp( tx, "2suff" ) );
            entityWithPropId( tx, "skruff" );
            IndexDescriptor index = tx.schemaRead().indexGetForName( INDEX_NAME );
            assertEntityAndValueForSeek( expected, tx, index, needsValues, "pasuff", IndexQuery.stringSuffix( prop, stringValue( "suff" ) ) );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldPerformScan( boolean needsValues ) throws Exception
    {
        // given
        Set<Pair<Long,Value>> expected = new HashSet<>();
        long entityToDelete;
        long entityToChange;
        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( entityWithProp( tx, "suff1" ) );
            expected.add( entityWithProp( tx, "supp" ) );
            entityToDelete = entityWithPropId( tx, "supp" );
            entityToChange = entityWithPropId( tx, "supper" );
            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( entityWithProp( tx, "suff2" ) );
            deleteEntity( tx, entityToDelete );
            removeProperty( tx, entityToChange );

            IndexDescriptor index = tx.schemaRead().indexGetForName( INDEX_NAME );

            assertEntityAndValueForScan( expected, tx, index, needsValues, "noff" );
        }
    }

    @Test
    void shouldPerformEqualitySeek() throws Exception
    {
        // given
        Set<Pair<Long,Value>> expected = new HashSet<>();
        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( entityWithProp( tx, "banana" ) );
            entityWithProp( tx, "apple" );
            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( entityWithProp( tx, "banana" ) );
            entityWithProp( tx, "dragonfruit" );
            IndexDescriptor index = tx.schemaRead().indexGetForName( INDEX_NAME );
            // Equality seek does never provide values
            int prop = tx.tokenRead().propertyKey( DEFAULT_PROPERTY_NAME );
            assertEntityAndValueForSeek( expected, tx, index, false, "banana", IndexQuery.exact( prop, "banana" ) );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldPerformStringPrefixSearch( boolean needsValues ) throws Exception
    {
        // given
        Set<Pair<Long,Value>> expected = new HashSet<>();
        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( entityWithProp( tx, "suff1" ) );
            entityWithPropId( tx, "supp" );
            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            int prop = tx.tokenRead().propertyKey( DEFAULT_PROPERTY_NAME );
            expected.add( entityWithProp( tx, "suff2" ) );
            entityWithPropId( tx, "skruff" );
            IndexDescriptor index = tx.schemaRead().indexGetForName( INDEX_NAME );

            assertEntityAndValueForSeek( expected, tx, index,  needsValues, "suffpa", IndexQuery.stringPrefix( prop, stringValue( "suff" ) ) );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldPerformStringRangeSearch( boolean needsValues ) throws Exception
    {
        // given
        Set<Pair<Long,Value>> expected = new HashSet<>();
        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( entityWithProp( tx, "banana" ) );
            entityWithProp( tx, "apple" );
            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            int prop = tx.tokenRead().propertyKey( DEFAULT_PROPERTY_NAME );
            expected.add( entityWithProp( tx, "cherry" ) );
            entityWithProp( tx, "dragonfruit" );
            IndexDescriptor index = tx.schemaRead().indexGetForName( INDEX_NAME );
            assertEntityAndValueForSeek( expected, tx, index, needsValues, "berry", IndexQuery.range( prop, "b", true, "d", false ) );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldPerformStringRangeSearchWithAddedEntityInTxState( boolean needsValues ) throws Exception
    {
        // given
        Set<Pair<Long,Value>> expected = new HashSet<>();
        long entityToChange;
        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( entityWithProp( tx, "banana" ) );
            entityToChange = entityWithPropId( tx, "apple" );
            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {

            expected.add( entityWithProp( tx, "cherry" ) );
            entityWithProp( tx, "dragonfruit" );
            IndexDescriptor index = tx.schemaRead().indexGetForName( INDEX_NAME );
            TextValue newProperty = stringValue( "blueberry" );
            setProperty( tx, entityToChange, newProperty );
            expected.add(Pair.of(entityToChange, newProperty ));

            int prop = tx.tokenRead().propertyKey( DEFAULT_PROPERTY_NAME );
            assertEntityAndValueForSeek( expected, tx, index, needsValues, "berry", IndexQuery.range( prop, "b", true, "d", false ) );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldPerformStringRangeSearchWithChangedEntityInTxState( boolean needsValues ) throws Exception
    {
        // given
        Set<Pair<Long,Value>> expected = new HashSet<>();
        long entityToChange;
        try ( KernelTransaction tx = beginTransaction() )
        {
            entityToChange = entityWithPropId( tx, "banana" );
            entityWithPropId( tx, "apple" );
            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( entityWithProp( tx, "cherry" ) );
            entityWithProp( tx, "dragonfruit" );
            IndexDescriptor index = tx.schemaRead().indexGetForName( INDEX_NAME );
            TextValue newProperty = stringValue( "kiwi" );
            setProperty( tx, entityToChange, newProperty );

            int prop = tx.tokenRead().propertyKey( DEFAULT_PROPERTY_NAME );
            assertEntityAndValueForSeek( expected, tx, index, needsValues, "berry", IndexQuery.range( prop, "b", true, "d", false ) );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldPerformStringRangeSearchWithRemovedRemovedPropertyInTxState( boolean needsValues ) throws Exception
    {
        // given
        Set<Pair<Long,Value>> expected = new HashSet<>();
        long entityToChange;
        try ( KernelTransaction tx = beginTransaction() )
        {
            entityToChange = entityWithPropId( tx, "banana" );
            entityWithPropId( tx, "apple" );
            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( entityWithProp( tx, "cherry" ) );
            entityWithProp( tx, "dragonfruit" );
            IndexDescriptor index = tx.schemaRead().indexGetForName( INDEX_NAME );
            removeProperty( tx, entityToChange );

            int prop = tx.tokenRead().propertyKey( DEFAULT_PROPERTY_NAME );
            assertEntityAndValueForSeek( expected, tx, index, needsValues, "berry", IndexQuery.range( prop, "b", true, "d", false ) );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldPerformStringRangeSearchWithDeletedEntityInTxState( boolean needsValues ) throws Exception
    {
        // given
        Set<Pair<Long,Value>> expected = new HashSet<>();
        long entityToChange;
        try ( KernelTransaction tx = beginTransaction() )
        {
            entityToChange = entityWithPropId( tx, "banana" );
            entityWithPropId( tx, "apple" );
            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            int prop = tx.tokenRead().propertyKey( DEFAULT_PROPERTY_NAME );
            expected.add( entityWithProp( tx, "cherry" ) );
            entityWithProp( tx, "dragonfruit" );
            IndexDescriptor index = tx.schemaRead().indexGetForName( INDEX_NAME );
            deleteEntity( tx, entityToChange );

            assertEntityAndValueForSeek( expected, tx, index, needsValues, "berry", IndexQuery.range( prop, "b", true, "d", false ) );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldPerformStringContainsSearch( boolean needsValues ) throws Exception
    {
        // given
        Set<Pair<Long,Value>> expected = new HashSet<>();
        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( entityWithProp( tx, "gnomebat" ) );
            entityWithPropId( tx, "fishwombat" );
            tx.commit();
        }

        createIndex();

        // when
        try ( KernelTransaction tx = beginTransaction() )
        {
            expected.add( entityWithProp( tx, "homeopatic" ) );
            entityWithPropId( tx, "telephonecompany" );
            IndexDescriptor index = tx.schemaRead().indexGetForName( INDEX_NAME );

            int prop = tx.tokenRead().propertyKey( DEFAULT_PROPERTY_NAME );
            assertEntityAndValueForSeek( expected, tx, index, needsValues, "immense", IndexQuery.stringContains( prop, stringValue( "me" ) ) );
        }
    }

    @Test
    void shouldThrowIfTransactionTerminated() throws Exception
    {
        try ( KernelTransaction tx = beginTransaction() )
        {
            // given
            terminate( tx );

            // when
            assertThrows( TransactionTerminatedException.class, () -> entityExists( tx, 42 ) );
        }
    }

    @Override
    public WriteTestSupport newTestSupport()
    {
        return new WriteTestSupport();
    }

    private void terminate( KernelTransaction transaction )
    {
        transaction.markForTermination( Status.Transaction.Terminated );
    }

    long entityWithPropId( KernelTransaction tx, Object value ) throws Exception
    {
        return entityWithProp( tx, value ).first();
    }

    void assertEntityAndValue( Set<Pair<Long,Value>> expected, KernelTransaction tx, boolean needsValues, Object anotherValueFoundByQuery,
            EntityValueIndexCursor entities ) throws Exception
    {
        // Modify tx state with changes that should not be reflected in the cursor,
        // since the cursor was already initialized in the code calling this method
        for ( Pair<Long,Value> pair : expected )
        {
            tx.dataWrite().relationshipDelete( pair.first() );
        }
        entityWithPropId( tx, anotherValueFoundByQuery );

        if ( needsValues )
        {
            Set<Pair<Long,Value>> found = new HashSet<>();
            while ( entities.next() )
            {
                found.add( Pair.of( entities.entityReference(), entities.propertyValue( 0 ) ) );
            }

            assertThat( found ).isEqualTo( expected );
        }
        else
        {
            Set<Long> foundIds = new HashSet<>();
            while ( entities.next() )
            {
                foundIds.add( entities.entityReference() );
            }
            ImmutableSet<Long> expectedIds = expected.stream().map( Pair::first ).collect( Collectors2.toImmutableSet() );

            assertThat( foundIds ).isEqualTo( expectedIds );
        }
    }

    abstract Pair<Long,Value> entityWithProp( KernelTransaction tx, Object value ) throws Exception;

    abstract void createIndex();

    abstract void deleteEntity( KernelTransaction tx, long entity ) throws Exception;

    abstract boolean entityExists( KernelTransaction tx, long entity );

    abstract void removeProperty( KernelTransaction tx, long entity  ) throws Exception;

    abstract void setProperty( KernelTransaction tx, long entity, Value value ) throws Exception;

    /**
     * Perform an index seek and assert that the correct entities and values were found.
     *
     * Since this method modifies TX state for the test it is not safe to call this method more than once in the same transaction.
     *
     * @param expected the expected entities and values
     * @param tx the transaction
     * @param index the index
     * @param needsValues if the index is expected to provide values
     * @param anotherValueFoundByQuery a values that would be found by the index queries, if a entity with that value existed. This method
     * will create a entity with that value, after initializing the cursor and assert that the new entity is not found.
     * @param queries the index queries
     */
    abstract void assertEntityAndValueForSeek( Set<Pair<Long,Value>> expected, KernelTransaction tx, IndexDescriptor index, boolean needsValues,
            Object anotherValueFoundByQuery, IndexQuery... queries ) throws Exception;

    /**
     * Perform an index scan and assert that the correct entities and values were found.
     *
     * Since this method modifies TX state for the test it is not safe to call this method more than once in the same transaction.
     *
     * @param expected the expected entities and values
     * @param tx the transaction
     * @param index the index
     * @param needsValues if the index is expected to provide values
     * @param anotherValueFoundByQuery a values that would be found by, if a entity with that value existed. This method
     * will create a entity with that value, after initializing the cursor and assert that the new entity is not found.
     */
    abstract void assertEntityAndValueForScan( Set<Pair<Long,Value>> expected, KernelTransaction tx, IndexDescriptor index, boolean needsValues,
            Object anotherValueFoundByQuery ) throws Exception;

    interface EntityValueIndexCursor
    {
        boolean next();

        Value propertyValue( int offset );

        long entityReference();
    }
}
