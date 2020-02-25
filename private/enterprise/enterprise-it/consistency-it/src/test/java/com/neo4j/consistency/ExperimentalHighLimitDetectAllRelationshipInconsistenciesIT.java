/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.consistency;

import com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimitWithSmallRecords;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.consistency.newchecker.full.ExperimentalDetectAllRelationshipInconsistenciesIT;
import org.neo4j.graphdb.config.Setting;

public class ExperimentalHighLimitDetectAllRelationshipInconsistenciesIT extends ExperimentalDetectAllRelationshipInconsistenciesIT
{
    @Override
    protected String getRecordFormatName()
    {
        return HighLimitWithSmallRecords.NAME;
    }

    @Override
    protected Map<Setting<?>,Object> getConfig()
    {
        Map<Setting<?>,Object> cfg = new HashMap<>( super.getConfig() );
        cfg.put( MetricsSettings.metricsEnabled, false );
        cfg.put( OnlineBackupSettings.online_backup_enabled, false );
        return cfg;
    }
}
