package org.neo4j.internal.recordstorage.CSR;

import org.neo4j.internal.recordstorage.RecordPropertyCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.format.CSR.CSRBaseUtils;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.storable.Value;

import java.util.ArrayList;

public class CSRRecordPropertyCursor extends RecordPropertyCursor {

    private OWNER_TYPE owner_type = OWNER_TYPE.NONE;
    private long owner = NO_ID;
    CSRBaseUtils utils;
    ArrayList<NeoStores> neoStores;

    public CSRRecordPropertyCursor(ArrayList<NeoStores> neoStores, PageCursorTracer cursorTracer, MemoryTracker memoryTracker) {
        super(neoStores.get( 0 ).getPropertyStore(), cursorTracer, memoryTracker);
        this.neoStores = neoStores;
        utils = CSRBaseUtils.getInstance( propertyStore.getParentNeoStore(), null );
    }

    public void initNodeProperties( long reference, long owner )
    {
        this.owner = owner;
        this.owner_type = OWNER_TYPE.NODE;
        init( reference );
    }

    public void initRelationshipProperties( long reference, long owner )
    {
        this.owner = owner;
        this.owner_type = OWNER_TYPE.RELATIONSHIP;
        init( reference );
    }
    @Override
    public OWNER_TYPE getOwnerType()
    {
        return owner_type;
    }

    @Override
    public long getOwner()
    {
        return owner;
    }

    @Override
    protected void init( long reference )
    {
        if ( getId() != NO_ID )
        {
            clear();
        }
        // Store state
        this.next = reference;
        this.open = true;
    }

    private int currentPropertyKey;
    private Value currentPropertyValue;
    @Override
    public boolean next()
    {
        if (next == -1 || next == 0)
            return false;
        currentPropertyValue = utils.getPropertyValue((int)(next & 0x00000000_000000FFl), owner);
        currentPropertyKey = (int)(next & 0x00000000_000000FFl);
        next = next >> 8;
        return true;
    }

    @Override
    public Value propertyValue()
    {
        return currentPropertyValue;
    }

    @Override
    public int propertyKey()
    {
        return currentPropertyKey;
    }
}
