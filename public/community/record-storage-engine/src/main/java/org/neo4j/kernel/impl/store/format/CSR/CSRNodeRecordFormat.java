package org.neo4j.kernel.impl.store.format.CSR;
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

import org.neo4j.internal.batchimport.ImportLogic;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.format.BaseRecordFormat;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import java.util.Iterator;

public class CSRNodeRecordFormat extends BaseOneByteHeaderRecordFormat<NodeRecord>
{
    // in_use(byte)+next_rel_id(int)+next_prop_id(int)+labels(5)+extra(byte)
    //public static final int RECORD_SIZE = 15;

    public CSRNodeRecordFormat()
    {
        this( false, null );
    }

    /*NeoStores neoStores = null;
    NodeMapping nodeMapping;
    CSRGraphStore csrGraphStore;
    NodeStore nodeStore;
    CSRGraph csgGraph;
    boolean csrSetupDone = false;
    HashMap<String, Integer> labelMap = new HashMap<String, Integer>(), propertyKeyMap = new HashMap<String, Integer>();*/
    CSRBaseUtils utils;
    public CSRNodeRecordFormat(boolean pageAligned, NeoStores neoStores )
    {
        //super( fixedRecordSize(1), 1, IN_USE_BIT, 35, pageAligned );
        super( fixedRecordSize( 15 ), 0, IN_USE_BIT, 35, pageAligned );
        //this.neoStores = neoStores;
        //csrSetup();
        utils = CSRBaseUtils.getInstance( neoStores );
    }

    /*private void csrSetup()
    {
        if (csrSetupDone)
            return;
        if (neoStores != null) {
            csrGraphStore = neoStores.getCSRGraphStore();
            if (csrGraphStore != null) {
                csgGraph = (CSRGraph) csrGraphStore.getGraph();
                nodeMapping = csrGraphStore.nodes();
                nodeStore = NodeStore.of(csrGraphStore, AllocationTracker.empty());
                getLabelMap();
                getPropertyKeyMap();
                csrSetupDone = true;
            }
        }
    }

    private void getLabelMap()
    {
        try {
            Iterator<NamedToken> tokens = neoStores.getLabelTokenStore().getTokens(PageCursorTracer.NULL).iterator();
            while (tokens.hasNext()) {
                NamedToken token = tokens.next();
                labelMap.put(token.name(), token.id());
            }
        } catch (IllegalStateException e)
        {
            //do nothing - store not available.
        }
    }
    private void getPropertyKeyMap()
    {
        try {
            Iterator<NamedToken> tokens = neoStores.getPropertyKeyTokenStore().getTokens(PageCursorTracer.NULL).iterator();
            while (tokens.hasNext()) {
                NamedToken token = tokens.next();
                propertyKeyMap.put(token.name(), token.id());
            }
        } catch (IllegalStateException e)
        {
            //do nothing - store not available.
        }
    }
*/
    @Override
    public NodeRecord newRecord()
    {
        return new NodeRecord( -1 );
    }

    @Override
    public void read( NodeRecord record, PageCursor cursor, RecordLoad mode, int recordSize, int recordsPerPage ) {
        if (!ImportLogic.IMPORT_IN_PROGRESS) {
            utils.csrSetup();
            if (utils.csrSetupDone) {
                long nodeId = record.getId();
                long gdsId = utils.nodeMapping.toMappedNodeId(nodeId);
                if (gdsId == -1) {
                    record.setInUse(false);
                    return;
                }
                String[] labels = utils.nodeStore.labels(gdsId);
                //for (int i = 0; i < labels.length; i++)
                //    if (!labels[i].equalsIgnoreCase("item"))
                //        System.out.println("Customer found ["+gdsId+"]["+labels[i]+"]");
                record.setInUse(true);
                long[] labelIds = new long[labels.length];
                for (int i = 0; i < labels.length; i++) {
                    //if (utils.labelMap.size() == 0)
                   //    utils.getLabelMap();
                    if (utils.labelMap.size() != 0)
                        labelIds[i] = utils.labelMap.get(labels[i]);
                }

                InlineNodeLabels.putSorted(record, labelIds, null, utils.neoStores.getNodeStore().getDynamicLabelStore(), PageCursorTracer.NULL, null);

                //find the outgoing degree of the node
                if (utils.csrHugeGraph.degree( gdsId ) == 0)
                    record.setNextRel(AbstractBaseRecord.NO_ID);
                else
                    record.setNextRel(utils.AOForRelationshipID.get( gdsId ));
                        //((HugeGraph) utils.csrHugeGraph).getAdjacencyOffsets().get(nodeId));

                //set property
                long longKey = AbstractBaseRecord.NO_ID;
                Iterator<String> propKeys = utils.propertyKeyMap.keySet().iterator();
                while (propKeys.hasNext()) {
                    String keyName = propKeys.next();
                    if (utils.csrHugeGraph.nodeProperties(keyName).value(nodeId) != null) {
                        long key = (long) utils.propertyKeyMap.get(keyName);
                        longKey <<= 8;
                        longKey = longKey | key;
                    }
                }
                record.setNextProp(longKey);
            }
        }
         else read1(record, cursor, mode, recordSize, recordsPerPage);
/*
        ///=======
        visitor.id(relationshipStore.nodeLabels.toOriginalNodeId(id));
        public boolean id( long id )
        {
            nodeRecord.setId( id );
            highestId = max( highestId, id );
            return true;
        }

        //=====

        if (hasLabels) {
            String[] labels = relationshipStore.labels(id);
            visitor.labels(labels);
            //===
            public boolean labels( String[] labels )
            {
                assert !hasLabelField;
                int requiredLength = labelsCursor + labels.length;
                if ( requiredLength > this.labels.length )
                {
                    this.labels = copyOf( this.labels, Integer.max( requiredLength, this.labels.length * 2 ) );
                }
                System.arraycopy( labels, 0, this.labels, labelsCursor, labels.length );
                labelsCursor += labels.length;
                return true;
            }
            //===

            if (hasProperties) {
                for (var label : labels) {
                    if (relationshipStore.nodeProperties.containsKey(label)) {
                        for (var propertyKeyAndValue : relationshipStore.nodeProperties.get(label).entrySet()) {
                            exportProperty(visitor, propertyKeyAndValue);
                        }
                    }
                }
            }
        } else if (hasProperties) { // no label information, but node properties
            for (var propertyKeyAndValue : relationshipStore.nodeProperties.get(ALL_NODES.name).entrySet()) {
                exportProperty(visitor, propertyKeyAndValue);
            }
        }

        visitor.endOfEntity();
        ///
        private void exportProperty(InputEntityVisitor visitor, Map.Entry<String, NodeProperties> propertyKeyAndValue) {
            var value = propertyKeyAndValue.getValue().getObject(id);
            if (value != null) {
                visitor.property(propertyKeyAndValue.getKey(), value);
            }
        }
*/
    }
    //@Override
    public void read1( NodeRecord record, PageCursor cursor, RecordLoad mode, int recordSize, int recordsPerPage )
    {
        byte headerByte = cursor.getByte();
        boolean inUse = isInUse( headerByte );
        record.setInUse( inUse );
        if ( mode.shouldLoad( inUse ) )
        {
            long nextRel = cursor.getInt() & 0xFFFFFFFFL;
            long nextProp = cursor.getInt() & 0xFFFFFFFFL;

            long relModifier = (headerByte & 0xEL) << 31;
            long propModifier = (headerByte & 0xF0L) << 28;

            long lsbLabels = cursor.getInt() & 0xFFFFFFFFL;
            long hsbLabels = cursor.getByte() & 0xFF; // so that a negative byte won't fill the "extended" bits with ones.
            long labels = lsbLabels | (hsbLabels << 32);
            byte extra = cursor.getByte();
            boolean dense = (extra & 0x1) > 0;

            record.initialize( inUse,
                    BaseRecordFormat.longFromIntAndMod( nextProp, propModifier ), dense,
                    BaseRecordFormat.longFromIntAndMod( nextRel, relModifier ), labels );
        }
        else
        {
            int nextOffset = cursor.getOffset() + recordSize - HEADER_SIZE;
            cursor.setOffset( nextOffset );
        }
    }

    @Override
    public void write( NodeRecord record, PageCursor cursor, int recordSize, int recordsPerPage )
    {
        if ( record.inUse() )
        {
            long nextRel = record.getNextRel();
            long nextProp = record.getNextProp();

            short relModifier = nextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (short)((nextRel & 0x700000000L) >> 31);
            short propModifier = nextProp == Record.NO_NEXT_PROPERTY.intValue() ? 0 : (short)((nextProp & 0xF00000000L) >> 28);

            // [    ,   x] in use bit
            // [    ,xxx ] higher bits for rel id
            // [xxxx,    ] higher bits for prop id
            short inUseUnsignedByte = ( record.inUse() ? Record.IN_USE : Record.NOT_IN_USE ).byteValue();
            inUseUnsignedByte = (short) ( inUseUnsignedByte | relModifier | propModifier );

            cursor.putByte( (byte) inUseUnsignedByte );
            cursor.putInt( (int) nextRel );
            cursor.putInt( (int) nextProp );

            // lsb of labels
            long labelField = record.getLabelField();
            cursor.putInt( (int) labelField );
            // msb of labels
            cursor.putByte( (byte) ((labelField & 0xFF00000000L) >> 32) );

            byte extra = record.isDense() ? (byte)1 : (byte)0;
            cursor.putByte( extra );
        }
        else
        {
            markAsUnused( cursor );
        }
    }
}
