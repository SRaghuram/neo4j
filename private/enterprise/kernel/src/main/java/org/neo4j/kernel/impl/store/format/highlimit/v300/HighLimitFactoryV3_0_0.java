/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.store.format.highlimit.v300;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.store.format.RecordFormats;

@Service.Implementation( RecordFormats.Factory.class )
public class HighLimitFactoryV3_0_0 extends RecordFormats.Factory
{
    public HighLimitFactoryV3_0_0()
    {
        super( HighLimitV3_0_0.NAME, HighLimitV3_0_0.STORE_VERSION );
    }

    @Override
    public RecordFormats newInstance()
    {
        return HighLimitV3_0_0.RECORD_FORMATS;
    }
}
