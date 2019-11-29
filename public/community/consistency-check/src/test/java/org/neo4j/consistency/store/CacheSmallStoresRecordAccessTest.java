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
package org.neo4j.consistency.store;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class CacheSmallStoresRecordAccessTest
{
    @Test
    void shouldDelegateLookupForMostStores()
    {
        // given
        RecordAccess delegate = mock( RecordAccess.class );
        CacheSmallStoresRecordAccess recordAccess = new CacheSmallStoresRecordAccess( delegate, null, null, null );

        // when
        recordAccess.node( 42 );
        recordAccess.relationship( 2001 );
        recordAccess.property( 2468 );
        recordAccess.string( 666 );
        recordAccess.array( 11 );

        // then
        verify( delegate ).node( 42 );
        verify( delegate ).relationship( 2001 );
        verify( delegate ).property( 2468 );
        verify( delegate ).string( 666 );
        verify( delegate ).array( 11 );
    }

    @Test
    void shouldServePropertyKeysAndRelationshipLabelsFromSuppliedArrayCaches()
    {
        // given
        RecordAccess delegate = mock( RecordAccess.class );
        PropertyKeyTokenRecord propertyKey0 = new PropertyKeyTokenRecord( 0 );
        PropertyKeyTokenRecord propertyKey2 = new PropertyKeyTokenRecord( 2 );
        PropertyKeyTokenRecord propertyKey1 = new PropertyKeyTokenRecord( 1 );
        RelationshipTypeTokenRecord relationshipType0 = new RelationshipTypeTokenRecord( 0 );
        RelationshipTypeTokenRecord relationshipType1 = new RelationshipTypeTokenRecord( 1 );
        RelationshipTypeTokenRecord relationshipType2 = new RelationshipTypeTokenRecord( 2 );
        LabelTokenRecord label0 = new LabelTokenRecord( 0 );
        LabelTokenRecord label1 = new LabelTokenRecord( 1 );
        LabelTokenRecord label2 = new LabelTokenRecord( 2 );

        CacheSmallStoresRecordAccess recordAccess = new CacheSmallStoresRecordAccess(
                delegate, new PropertyKeyTokenRecord[]{
                propertyKey0,
                propertyKey1,
                propertyKey2,
        }, new RelationshipTypeTokenRecord[]{
                relationshipType0,
                relationshipType1,
                relationshipType2,
        }, new LabelTokenRecord[]{
                label0,
                label1,
                label2,
        } );

        // when
        assertThat( getRecord( recordAccess.propertyKey( 0 ) ) ).isSameAs( propertyKey0 );
        assertThat( getRecord( recordAccess.propertyKey( 1 ) ) ).isSameAs( propertyKey1 );
        assertThat( getRecord( recordAccess.propertyKey( 2 ) ) ).isSameAs( propertyKey2 );
        assertThat( getRecord( recordAccess.relationshipType( 0 ) ) ).isSameAs( relationshipType0 );
        assertThat( getRecord( recordAccess.relationshipType( 1 ) ) ).isSameAs( relationshipType1 );
        assertThat( getRecord( recordAccess.relationshipType( 2 ) ) ).isSameAs( relationshipType2 );
        assertThat( getRecord( recordAccess.label( 0 ) ) ).isSameAs( label0 );
        assertThat( getRecord( recordAccess.label( 1 ) ) ).isSameAs( label1 );
        assertThat( getRecord( recordAccess.label( 2 ) ) ).isSameAs( label2 );

        verifyNoInteractions( delegate );
    }

    private static <T extends AbstractBaseRecord> AbstractBaseRecord getRecord( RecordReference<T> reference )
    {
        return ((DirectRecordReference) reference).record();
    }
}
