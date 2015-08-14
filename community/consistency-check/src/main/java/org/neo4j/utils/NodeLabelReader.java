/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.utils;

import static org.neo4j.kernel.impl.store.AbstractDynamicStore.readFullByteArrayFromHeavyRecords;
import static org.neo4j.kernel.impl.store.DynamicArrayStore.getRightArray;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.LabelChainWalker;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.kernel.impl.store.DynamicNodeLabels;
import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.LabelIdArray;
import org.neo4j.kernel.impl.store.NodeLabels;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.store.StoreAccess;

public class NodeLabelReader
{
    public static Set<Long> getListOfLabels(
            NodeRecord nodeRecord, StoreAccess access)
    {
        final Set<Long> labels = new HashSet<>();
        List<DynamicRecord> recordList = new ArrayList<>();
        
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( nodeRecord );
        if ( nodeLabels instanceof DynamicNodeLabels )
        {

            DynamicNodeLabels dynamicNodeLabels = (DynamicNodeLabels) nodeLabels;
            long recordId = dynamicNodeLabels.getFirstDynamicRecordId();
            DynamicRecord record = null;
            do 
            {
                record = access.getNodeDynamicLabelStore().forceGetRecord( recordId );
                long nextBlock = record.getNextBlock();
                if ( Record.NO_NEXT_BLOCK.is( nextBlock ) )
                    break;
                recordList.add( record );
            } while (true);
            copyToSet ( labelIds(recordList), labels );
        }
        else
        {
            copyToSet( nodeLabels.get( null ), labels );
        }

        return labels;
    }
    
    private static long[] labelIds( List<DynamicRecord> recordList )
    {
        long[] idArray =
                (long[]) getRightArray( readFullByteArrayFromHeavyRecords( recordList, PropertyType.ARRAY ) );
        return LabelIdArray.stripNodeId( idArray );
    }

    public static Set<Long> getListOfLabels(
            long labelField )
    {
        final Set<Long> labels = new HashSet<>();
        copyToSet( InlineNodeLabels.parseInlined(labelField), labels );

        return labels;
    }

    private static void copyToSet( long[] array, Set<Long> set )
    {
        for ( long labelId : array )
        {
            set.add( labelId );
        }
    }
}
