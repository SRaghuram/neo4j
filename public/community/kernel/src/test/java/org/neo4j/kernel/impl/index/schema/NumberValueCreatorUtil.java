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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.ValueGroup;

class NumberValueCreatorUtil extends ValueCreatorUtil<NumberIndexKey,NativeIndexValue>
{
    NumberValueCreatorUtil( StoreIndexDescriptor indexDescriptor )
    {
        super( indexDescriptor, RandomValues.typesOfGroup( ValueGroup.NUMBER ) );
    }

    @Override
    int compareIndexedPropertyValue( NumberIndexKey key1, NumberIndexKey key2 )
    {
        int typeCompare = Byte.compare( key1.type, key2.type );
        if ( typeCompare == 0 )
        {
            return Long.compare( key1.rawValueBits, key2.rawValueBits );
        }
        return typeCompare;
    }
}
