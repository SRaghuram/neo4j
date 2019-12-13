/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.consistency;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimitWithSmallRecords;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;

import java.io.File;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.newchecker.DetectRandomSabotageIT;
import org.neo4j.dbms.api.DatabaseManagementService;

class HighLimitDetectRandomSabotageIT extends DetectRandomSabotageIT
{
    @Override
    protected DatabaseManagementService getDbms( File home )
    {
        //Alternating between normal/small records to sometimes use fixed references and sometimes dynamic references
        String recordFormat = random.nextBoolean() ? HighLimitWithSmallRecords.NAME : HighLimit.NAME;

        return new TestEnterpriseDatabaseManagementServiceBuilder( home )
                .setConfig( GraphDatabaseSettings.record_format, recordFormat )
                .build();
    }
}
