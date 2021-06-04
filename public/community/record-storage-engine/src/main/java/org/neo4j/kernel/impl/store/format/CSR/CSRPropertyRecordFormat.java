package org.neo4j.kernel.impl.store.format.CSR;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.format.BaseRecordFormat;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import java.io.IOException;
import java.util.function.Function;

public class CSRPropertyRecordFormat extends BaseRecordFormat<PropertyRecord> {
    protected CSRPropertyRecordFormat(Function<StoreHeader, Integer> recordSize, int recordHeaderSize, int idBits, boolean pageAligned) {
        super(recordSize, recordHeaderSize, idBits, pageAligned);
    }

    @Override
    public PropertyRecord newRecord() {
        return null;
    }

    @Override
    public boolean isInUse(PageCursor cursor) {
        return false;
    }

    @Override
    public void read(PropertyRecord record, PageCursor cursor, RecordLoad mode, int recordSize, int recordsPerPage) throws IOException {

    }

    @Override
    public void write(PropertyRecord record, PageCursor cursor, int recordSize, int recordsPerPage) throws IOException {

    }
}
