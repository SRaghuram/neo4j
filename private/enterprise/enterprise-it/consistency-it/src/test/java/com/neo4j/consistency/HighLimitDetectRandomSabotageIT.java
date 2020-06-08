/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.consistency;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimitWithSmallRecords;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.newchecker.DetectRandomSabotageIT;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

class HighLimitDetectRandomSabotageIT extends DetectRandomSabotageIT
{
    private String recordFormat;

    @Override
    @BeforeEach
    protected void setUp()
    {
        // Decide on record format once per test such that both consistency checker and dbms use the same.
        // Alternating between normal/small records to sometimes use fixed references and sometimes dynamic references.
        recordFormat = random.nextBoolean() ? HighLimitWithSmallRecords.NAME : HighLimit.NAME;
        super.setUp();
    }

    @Override
    protected TestDatabaseManagementServiceBuilder createBuilder( File home )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( home );
    }

    @Override
    protected <T> T addConfig( T t, DetectRandomSabotageIT.SetConfigAction<T> action )
    {
        action.setConfig( t, GraphDatabaseSettings.record_format, recordFormat );
        return super.addConfig( t, action );
    }
}
