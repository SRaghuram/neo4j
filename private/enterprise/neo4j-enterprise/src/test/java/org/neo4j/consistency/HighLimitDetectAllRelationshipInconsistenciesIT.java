/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.consistency;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimitWithSmallRecords;

import org.neo4j.consistency.checking.full.DetectAllRelationshipInconsistenciesIT;

public class HighLimitDetectAllRelationshipInconsistenciesIT extends DetectAllRelationshipInconsistenciesIT
{
    @Override
    protected String getRecordFormatName()
    {
        return HighLimitWithSmallRecords.NAME;
    }

}
