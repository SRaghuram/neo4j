/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.configuration;

import java.util.Map;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.SettingMigrator;
import org.neo4j.logging.Log;

@ServiceProvider
public class MetricsSettingsMigrator implements SettingMigrator
{
    private static final String OLD_LOGS_METRICS_SETTING_NAME = "metrics.neo4j.logrotation.enabled";
    private static final String NEW_LOG_METRICS_SETTING_NAME = MetricsSettings.neoTransactionLogsEnabled.name();

    @Override
    public void migrate( Map<String,String> input, Log log )
    {
        String oldSettingValue = input.remove( OLD_LOGS_METRICS_SETTING_NAME );
        if ( oldSettingValue != null )
        {
            log.warn( "Use of deprecated setting %s. It is replaced by %s", OLD_LOGS_METRICS_SETTING_NAME, NEW_LOG_METRICS_SETTING_NAME );
            input.putIfAbsent( NEW_LOG_METRICS_SETTING_NAME, oldSettingValue );
        }
    }
}
