/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.consistency;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimitWithSmallRecords;

import org.neo4j.consistency.ConsistencyCheckServiceIntegrationTest;

public class HighLimitConsistencyCheckServiceIT extends ConsistencyCheckServiceIntegrationTest
{
    @Override
    protected String getRecordFormatName()
    {
        return HighLimitWithSmallRecords.NAME;
    }

}
