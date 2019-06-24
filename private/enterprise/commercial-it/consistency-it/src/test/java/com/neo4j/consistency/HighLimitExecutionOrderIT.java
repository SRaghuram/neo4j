/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.consistency;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimitWithSmallRecords;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.consistency.checking.full.ExecutionOrderIntegrationTest;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.configuration.SettingValueParsers.FALSE;

public class HighLimitExecutionOrderIT extends ExecutionOrderIntegrationTest
{

    @Override
    protected String getRecordFormatName()
    {
        return HighLimitWithSmallRecords.NAME;
    }

    @Override
    protected Map<Setting<?>,String> getSettings()
    {
        Map<Setting<?>, String> settings = new HashMap<>( super.getSettings() );
        settings.put( OnlineBackupSettings.online_backup_enabled, FALSE );
        return settings;
    }
}
