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

import org.neo4j.storageengine.api.AllRelationshipsScan;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;

import static org.neo4j.internal.freki.MutableNodeRecordData.externalRelationshipId;
import static org.neo4j.internal.freki.MutableNodeRecordData.internalRelationshipIdFromRelationshipId;
import static org.neo4j.internal.freki.MutableNodeRecordData.nodeIdFromRelationshipId;
import static org.neo4j.internal.freki.MutableNodeRecordData.otherNodeOf;
import static org.neo4j.internal.freki.MutableNodeRecordData.relationshipHasProperties;
import static org.neo4j.internal.freki.MutableNodeRecordData.relationshipIsOutgoing;
import static org.neo4j.internal.freki.StreamVByte.readLongs;

class FrekiRelationshipScanCursor extends FrekiRelationshipCursor implements StorageRelationshipScanCursor
{
    private long singleId;
    private boolean needsLoading;

    private long internalId;
    private long otherNode;
    private int type;
    private boolean hasProperties;
    private int relationshipPropertiesIndex;

    FrekiRelationshipScanCursor( SimpleStore mainStore )
    {
        super( mainStore );
    }

    @Override
    public boolean next()
    {
        if ( needsLoading && singleId != NULL )
        {
            needsLoading = false;
            if ( loadMainRecord( nodeIdFromRelationshipId( singleId ) ) )
            {
                readRelationshipTypes();
                return findRelationship();
            }
        }
        return false;
    }

    private boolean findRelationship()
    {
        long expectedInternalRelationshipId = internalRelationshipIdFromRelationshipId( singleId );
        for ( int t = 0; t < relationshipTypesInNode.length; t++ )
        {
            data.position( relationshipTypeOffset( t ) );
            int hasPropertiesIndex = -1;
            long[] relationshipGroupData = readLongs( data );
            for ( int d = 0; d < relationshipGroupData.length; d += 2 )
            {
                long otherNodeRaw = relationshipGroupData[d];
                boolean hasProperties = relationshipHasProperties( otherNodeRaw );
                if ( hasProperties )
                {
                    hasPropertiesIndex++;
                }
                long internalId = relationshipGroupData[d + 1];
                if ( internalId == expectedInternalRelationshipId && relationshipIsOutgoing( otherNodeRaw ) )
                {
                    this.internalId = internalId;
                    this.hasProperties = hasProperties;
                    otherNode = otherNodeOf( otherNodeRaw );
                    type = relationshipTypesInNode[t];
                    relationshipPropertiesIndex = hasPropertiesIndex;
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    int currentRelationshipPropertiesOffset()
    {
        return relationshipPropertiesOffset( data.position(), relationshipPropertiesIndex );
    }

    @Override
    public void reset()
    {
        super.reset();
        singleId = NULL;
        internalId = NULL;
        needsLoading = false;
        otherNode = NULL;
        type = -1;
        hasProperties = false;
        relationshipPropertiesIndex = -1;
    }

    @Override
    public void scan( int type )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public void scan()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public boolean scanBatch( AllRelationshipsScan scan, int sizeHint )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public void single( long reference )
    {
        this.singleId = reference;
        this.needsLoading = true;
    }

    @Override
    public int type()
    {
        return type;
    }

    @Override
    public long sourceNodeReference()
    {
        return record.id;
    }

    @Override
    public long targetNodeReference()
    {
        return otherNode;
    }

    @Override
    public boolean hasProperties()
    {
        return hasProperties;
    }

    @Override
    public long propertiesReference()
    {
        return 0;
    }

    @Override
    public void properties( StoragePropertyCursor propertyCursor )
    {
        if ( !hasProperties() )
        {
            propertyCursor.reset();
            return;
        }

        FrekiPropertyCursor frekiPropertyCursor = (FrekiPropertyCursor) propertyCursor;
        frekiPropertyCursor.initRelationshipProperties( this );
    }

    @Override
    public long entityReference()
    {
        return externalRelationshipId( record.id, internalId, otherNode, true );
    }
}
