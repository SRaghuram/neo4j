/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.consistency;

import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimitWithSmallRecords;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.consistency.ConsistencyCheckServiceIntegrationTest;
import org.neo4j.graphdb.config.Setting;

public class HighLimitConsistencyCheckServiceIT extends ConsistencyCheckServiceIntegrationTest
{
    @Override
    protected String getRecordFormatName()
    {
        return HighLimitWithSmallRecords.NAME;
    }

    @Override
    protected Map<Setting<?>,Object> settings()
    {
        Map<Setting<?>, Object> settings = new HashMap<>( super.settings() );
        settings.put( OnlineBackupSettings.online_backup_enabled, false );
        return settings;
    }
}
