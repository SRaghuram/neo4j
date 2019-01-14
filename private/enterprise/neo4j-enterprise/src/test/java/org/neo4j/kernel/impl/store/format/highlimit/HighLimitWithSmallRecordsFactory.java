/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.store.format.highlimit;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.store.format.RecordFormats;

@Service.Implementation( RecordFormats.Factory.class )
public class HighLimitWithSmallRecordsFactory extends RecordFormats.Factory
{
    public HighLimitWithSmallRecordsFactory()
    {
        super( HighLimitWithSmallRecords.NAME, HighLimitWithSmallRecords.STORE_VERSION );
    }

    @Override
    public RecordFormats newInstance()
    {
        return HighLimitWithSmallRecords.RECORD_FORMATS;
    }
}
