/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.ext.udc.impl;

import java.util.Timer;

import org.neo4j.ext.udc.UdcSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.udc.UsageData;

/**
 * Kernel extension for UDC, the Usage Data Collector. The UDC runs as a background
 * daemon, waking up once a day to collect basic usage information about a long
 * running graph database.
 * <p>
 * The first update is delayed to avoid needless activity during integration
 * testing and short-run applications. Subsequent updates are made at regular
 * intervals. Both times are specified in milliseconds.
 */
public class UdcKernelExtension extends LifecycleAdapter
{
    private Timer timer;
    private final UsageData usageData;
    private final Config config;
    private final DataSourceManager dataSourceManager;

    UdcKernelExtension( Config config, DataSourceManager dataSourceManager, UsageData usageData, Timer timer )
    {
        this.config = config;
        this.dataSourceManager = dataSourceManager;
        this.usageData = usageData;
        this.timer = timer;
    }

    @Override
    public void start()
    {
        if ( !config.get( UdcSettings.udc_enabled ) )
        {
            return;
        }

        int firstDelay = config.get( UdcSettings.first_delay );
        int interval = config.get( UdcSettings.interval );
        HostnamePort hostAddress = config.get(UdcSettings.udc_host);

        UdcInformationCollector collector = new DefaultUdcInformationCollector( config, dataSourceManager, usageData );
        UdcTimerTask task = new UdcTimerTask( hostAddress, collector );

        timer.scheduleAtFixedRate( task, firstDelay, interval );
    }

    @Override
    public void stop()
    {
        if ( timer != null )
        {
            timer.cancel();
            timer = null;
        }
    }

}
