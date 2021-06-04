package org.neo4j.kernel.impl.store.format.CSR;

import org.neo4j.internal.batchimport.ImportLogic;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.format.BaseRecordFormat;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import java.io.IOException;

public class CSRRelationshipRecordFormat extends BaseOneByteHeaderRecordFormat<RelationshipRecord> {
    CSRBaseUtils utils;
    public CSRRelationshipRecordFormat(boolean pageAligned, NeoStores neoStores )
    {
        //super( fixedRecordSize(1), 1, IN_USE_BIT, 35, pageAligned );
        super( fixedRecordSize( 34 ), 0, IN_USE_BIT, 35, pageAligned );
        utils = CSRBaseUtils.getInstance( neoStores );
    }

    @Override
    public RelationshipRecord newRecord() {
        return new RelationshipRecord( -1 );
    }

    @Override
    public void read(RelationshipRecord record, PageCursor cursor, RecordLoad mode, int recordSize, int recordsPerPage) throws IOException {
        if (!ImportLogic.IMPORT_IN_PROGRESS) {
            utils.csrSetup();
            if (utils.csrSetupDone) {
                // id is set from the node record as next rel - points the first entry in adjacency list for that node
                long id = record.getId();
                record.setInUse( true );
                //source node
                long sourceNodeId = utils.getSourceId( id );
                record.setFirstNode(utils.nodeMapping.toOriginalNodeId(sourceNodeId));
                //target node
                long targetNodeId = utils.getTargetNodeId( sourceNodeId, id);
                while (targetNodeId == -1)
                    targetNodeId = utils.getTargetNodeId( sourceNodeId, id);
                record.setSecondNode(utils.nodeMapping.toOriginalNodeId(targetNodeId));
                //source chain - get the first and last of the chain
                long firstRelId = utils.AOForRelationshipID.get( sourceNodeId );
                try {
                    int offset = utils.getAOOffset(sourceNodeId, targetNodeId);
                } catch (Exception e)
                {
                    System.out.println("No Harm done: just continue-"+e.getMessage());
                }
                long lastRelId = sourceNodeId == utils.csrHugeGraph.nodeCount() -1? utils.csrHugeGraph.relationshipCount() -1 :  utils.AOForRelationshipID.get(sourceNodeId + 1)-1;
                if (id <= firstRelId) {
                    record.setFirstPrevRel(AbstractBaseRecord.NO_ID);
                    record.setFirstInFirstChain( true );
                } else {
                    record.setFirstPrevRel(id - 1);
                    record.setFirstInFirstChain( false );
                }
                if (id >= lastRelId) {
                    record.setFirstNextRel(AbstractBaseRecord.NO_ID);
                } else
                    record.setFirstNextRel(id + 1);

                //target chain
                record.setSecondNextRel( utils.getNextIncomingRelID( targetNodeId, id));
                long secondPrevRel = utils.getPreviousIncomingRelID( targetNodeId, id);
                record.setSecondPrevRel(secondPrevRel);//AbstractBaseRecord.NO_ID);
                record.setFirstInSecondChain( secondPrevRel == AbstractBaseRecord.NO_ID );
                //first property
                //if (utils.csrHugeGraph.getPropertyOffsets() != null)
                //    record.setNextProp(utils.csrHugeGraph.getPropertyOffsets().get(sourceNodeId));
                record.setNextProp( -1 );
                record.setType( 0 );
            }
        }
        else
            read1(record, cursor, mode, recordSize, recordsPerPage);
    }
    /*public void readOld(RelationshipRecord record, PageCursor cursor, RecordLoad mode, int recordSize, int recordsPerPage) throws IOException {
        if (!ImportLogic.IMPORT_IN_PROGRESS) {
            utils.csrSetup();
            if (utils.csrSetupDone) {
                // id is set from the node record as next rel - points the first entry in adjacency list for that node
                long id = record.getId();
                //relationshipType
                //csrGraphStore.relationshipTypes();
                //source node
                long firstNodeId = utils.csrHugeGraph.getAdjacencyOffsets().getGDSId(id, utils.csrGraphStore.nodes().nodeCount());
                long secondNodeId = utils.csrHugeGraph.getAdjacencyList().cursor(id).nextLong();
                record.setFirstNode(firstNodeId);
                //target node
                record.setSecondNode(secondNodeId);
                //source chain
                long firstIndexInAL = utils.csrHugeGraph.getAdjacencyOffsets().get(firstNodeId);
                long nextIndexInAL = utils.csrHugeGraph.getAdjacencyOffsets().get(firstNodeId + 1);
                if ((id - 1) < firstIndexInAL) {
                    record.setFirstNextRel(AbstractBaseRecord.NO_ID);
                } else
                    record.setFirstNextRel(id - 1);
                if (id + 1 >= nextIndexInAL) {
                    record.setFirstPrevRel(AbstractBaseRecord.NO_ID);
                } else
                    record.setFirstPrevRel(id + 1);
                //target chain
                record.setSecondNextRel(AbstractBaseRecord.NO_ID);
                record.setSecondPrevRel(AbstractBaseRecord.NO_ID);
                //first property
                if (utils.csrHugeGraph.getPropertyOffsets() != null)
                    record.setNextProp(utils.csrHugeGraph.getPropertyOffsets().get(firstNodeId));


            }
        }
        else
        read1(record, cursor, mode, recordSize, recordsPerPage);
    }
*/
    public void read1( RelationshipRecord record, PageCursor cursor, RecordLoad mode, int recordSize, int recordsPerPage )
    {
        byte headerByte = cursor.getByte();
        boolean inUse = isInUse( headerByte );
        record.setInUse( inUse );
        if ( mode.shouldLoad( inUse ) )
        {
            // [    ,   x] in use flag
            // [    ,xxx ] first node high order bits
            // [xxxx,    ] next prop high order bits
            long firstNode = cursor.getInt() & 0xFFFFFFFFL;
            long firstNodeMod = (headerByte & 0xEL) << 31;

            long secondNode = cursor.getInt() & 0xFFFFFFFFL;

            // [ xxx,    ][    ,    ][    ,    ][    ,    ] second node high order bits,     0x70000000
            // [    ,xxx ][    ,    ][    ,    ][    ,    ] first prev rel high order bits,  0xE000000
            // [    ,   x][xx  ,    ][    ,    ][    ,    ] first next rel high order bits,  0x1C00000
            // [    ,    ][  xx,x   ][    ,    ][    ,    ] second prev rel high order bits, 0x380000
            // [    ,    ][    , xxx][    ,    ][    ,    ] second next rel high order bits, 0x70000
            // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
            long typeInt = cursor.getInt();
            long secondNodeMod = (typeInt & 0x70000000L) << 4;
            int type = (int)(typeInt & 0xFFFF);

            long firstPrevRel = cursor.getInt() & 0xFFFFFFFFL;
            long firstPrevRelMod = (typeInt & 0xE000000L) << 7;

            long firstNextRel = cursor.getInt() & 0xFFFFFFFFL;
            long firstNextRelMod = (typeInt & 0x1C00000L) << 10;

            long secondPrevRel = cursor.getInt() & 0xFFFFFFFFL;
            long secondPrevRelMod = (typeInt & 0x380000L) << 13;

            long secondNextRel = cursor.getInt() & 0xFFFFFFFFL;
            long secondNextRelMod = (typeInt & 0x70000L) << 16;

            long nextProp = cursor.getInt() & 0xFFFFFFFFL;
            long nextPropMod = (headerByte & 0xF0L) << 28;

            byte extraByte = cursor.getByte();

            record.initialize( inUse,
                    BaseRecordFormat.longFromIntAndMod( nextProp, nextPropMod ),
                    BaseRecordFormat.longFromIntAndMod( firstNode, firstNodeMod ),
                    BaseRecordFormat.longFromIntAndMod( secondNode, secondNodeMod ),
                    type,
                    BaseRecordFormat.longFromIntAndMod( firstPrevRel, firstPrevRelMod ),
                    BaseRecordFormat.longFromIntAndMod( firstNextRel, firstNextRelMod ),
                    BaseRecordFormat.longFromIntAndMod( secondPrevRel, secondPrevRelMod ),
                    BaseRecordFormat.longFromIntAndMod( secondNextRel, secondNextRelMod ),
                    (extraByte & 0x1) != 0,
                    (extraByte & 0x2) != 0 );
        }
        else
        {
            int nextOffset = cursor.getOffset() + recordSize - HEADER_SIZE;
            cursor.setOffset( nextOffset );
        }
    }
    @Override
    public void write(RelationshipRecord record, PageCursor cursor, int recordSize, int recordsPerPage) throws IOException {

    }
}
