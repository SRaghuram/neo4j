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
package org.neo4j.internal.freki;

import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;

import static org.neo4j.internal.freki.StreamVByte.readIntDeltas;

class FrekiNodeCursor extends FrekiMainStoreCursor implements StorageNodeCursor
{
    private long singleId;
    private long scanId;

    FrekiNodeCursor( Store mainStore )
    {
        super( mainStore );
    }

    @Override
    public long[] labels()
    {
        data.position( labelsOffset );
        return readIntDeltas( new StreamVByte.LongArrayTarget(), data ).array();
    }

    @Override
    public boolean hasLabel( int label )
    {
        for ( long labelId : labels() )
        {
            if ( label == labelId )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public long relationshipGroupReference()
    {
        return 0;
    }

    @Override
    public long allRelationshipsReference()
    {
        return 0;
    }

    @Override
    public boolean isDense()
    {
        return false;
    }

    @Override
    public void scan()
    {
        scanId = 0;
    }

    @Override
    public boolean scanBatch( AllNodeScan scan, int sizeHint )
    {
        return false;
    }

    @Override
    public void single( long reference )
    {
        singleId = reference;
    }

    @Override
    public boolean hasProperties()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public long propertiesReference()
    {
        return entityReference();
    }

    @Override
    public void properties( StoragePropertyCursor propertyCursor )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public long entityReference()
    {
        return record.id;
    }

    @Override
    public boolean next()
    {
        if ( singleId != -1 )
        {
            loadMainRecord( singleId );
            singleId = -1;
            return record.hasFlag( Record.FLAG_IN_USE );
        }
        return false;
    }

    @Override
    public void reset()
    {
        super.reset();
        singleId = -1;
        scanId = -1;
    }
}
