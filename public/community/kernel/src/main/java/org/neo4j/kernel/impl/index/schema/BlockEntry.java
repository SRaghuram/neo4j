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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractKeySize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractValueSize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putKeyValueSize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.readKeyValueSize;

class BlockEntry<KEY,VALUE>
{
    private KEY key;
    private VALUE value;

    BlockEntry( KEY key, VALUE value )
    {
        this.key = key;
        this.value = value;
    }

    KEY key()
    {
        return key;
    }

    VALUE value()
    {
        return value;
    }

    static <KEY, VALUE> BlockEntry<KEY,VALUE> read( PageCursor pageCursor, Layout<KEY,VALUE> layout )
    {
        KEY key = layout.newKey();
        VALUE value = layout.newValue();
        read( pageCursor, layout, key, value );
        return new BlockEntry<>( key, value );
    }

    static <KEY, VALUE> void read( PageCursor pageCursor, Layout<KEY,VALUE> layout, KEY key, VALUE value )
    {
        long entrySize = readKeyValueSize( pageCursor );
        layout.readKey( pageCursor, key, extractKeySize( entrySize ) );
        layout.readValue( pageCursor, value, extractValueSize( entrySize ) );
    }

    static <KEY, VALUE> void write( PageCursor pageCursor, Layout<KEY,VALUE> layout, BlockEntry<KEY,VALUE> entry )
    {
        write( pageCursor, layout, entry.key(), entry.value() );
    }

    static <KEY, VALUE> void write( PageCursor pageCursor, Layout<KEY,VALUE> layout, KEY key, VALUE value )
    {
        int keySize = layout.keySize( key );
        int valueSize = layout.valueSize( value );
        putKeyValueSize( pageCursor, keySize, valueSize );
        layout.writeKey( pageCursor, key );
        layout.writeValue( pageCursor, value );
    }
}
