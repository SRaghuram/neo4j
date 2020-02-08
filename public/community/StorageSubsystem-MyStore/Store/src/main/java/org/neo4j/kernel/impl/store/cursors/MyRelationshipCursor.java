package org.neo4j.kernel.impl.store.cursors;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.MyStore;
import org.neo4j.storageengine.api.RelationshipVisitor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipCursor;

import static org.neo4j.storageengine.api.StorageEntityScanCursor.NO_ID;

public abstract class MyRelationshipCursor implements RelationshipVisitor<RuntimeException>, StorageRelationshipCursor {
    protected enum GroupState
    {
        INCOMING,
        OUTGOING,
        LOOP,
        NONE
    }
    protected MyStore myStore;
    protected PageCursor pageCursor;
    protected long[] currentRelationship;
    protected long currentRelationshipId = NO_ID;
    public MyRelationshipCursor( MyStore store )
    {
        this.myStore = store;
    }
    @Override
    public void visit(long relationshipId, int typeId, long startNodeId, long endNodeId) throws RuntimeException {

    }

    @Override
    public int type() {
        return (int)currentRelationship[0];
    }

    @Override
    public long sourceNodeReference() {
        return currentRelationship[2];
    }

    @Override
    public long targetNodeReference() {
        return currentRelationship[3];
    }
    public long getFirstNextRel()
    {
        return currentRelationship[4];
    }
    public long getSecondNextRel()
    {
        return currentRelationship[5];
    }

    @Override
    public boolean hasProperties() {
        return currentRelationship[1] != NO_ID;
    }

    @Override
    public long propertiesReference() {
        return currentRelationship[1];
    }

    @Override
    public void properties(StoragePropertyCursor propertyCursor) {
        propertyCursor.initRelationshipProperties( currentRelationship[1] );
    }

    @Override
    public long entityReference() {
        return currentRelationshipId;
    }


    public boolean inUse()
    {
        return currentRelationship[0] != NO_ID;
    }

    protected void relationship(long reference, PageCursor pageCursor)
    {
        //read.getRecordByCursor( reference, pageCursor );
        currentRelationship = myStore.getCell( reference, MyStore.MyStoreType.RELATIONSHIP );
        currentRelationshipId = reference;
    }

    PageCursor relationshipPage( long reference )
    {
        //return read.openPageCursorForReading( reference );
        return myStore.openPageCursorForReading( reference, MyStore.MyStoreType.RELATIONSHIP );
    }
    void relationshipFull( long reference, PageCursor pageCursor )
    {
        // We need to load forcefully for relationship chain traversal since otherwise we cannot
        // traverse over relationship records which have been concurrently deleted
        // (flagged as inUse = false).
        // see
        //      org.neo4j.kernel.impl.store.RelationshipChainPointerChasingTest
        //      org.neo4j.kernel.impl.locking.RelationshipCreateDeleteIT
        //myStore.getRecordByCursor( reference, pageCursor );
        currentRelationship = myStore.getCell( reference, MyStore.MyStoreType.RELATIONSHIP );
        currentRelationshipId = reference;
    }
    public int getType()
    {
        return (int)currentRelationship[0];
    }
}
