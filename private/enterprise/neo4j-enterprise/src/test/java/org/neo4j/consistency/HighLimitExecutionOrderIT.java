/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.consistency;

import org.neo4j.consistency.checking.full.ExecutionOrderIntegrationTest;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimitWithSmallRecords;

public class HighLimitExecutionOrderIT extends ExecutionOrderIntegrationTest
{

    @Override
    protected String getRecordFormatName()
    {
        return HighLimitWithSmallRecords.NAME;
    }
}
