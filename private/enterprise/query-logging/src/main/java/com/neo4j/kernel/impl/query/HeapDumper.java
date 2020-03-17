/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.query;

import java.io.IOException;
import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;

import com.sun.management.HotSpotDiagnosticMXBean;

public class HeapDumper
{
    private static final HotSpotDiagnosticMXBean HOT_SPOT_DIAGNOSTIC_MX_BEAN = getHotspotDiagnosticMxBean();
    private static final String HOTSPOT_BEAN_NAME = "com.sun.management:type=HotSpotDiagnostic";

    private static HotSpotDiagnosticMXBean getHotspotDiagnosticMxBean()
    {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try
        {
            return ManagementFactory.newPlatformMXBeanProxy( server, HOTSPOT_BEAN_NAME, HotSpotDiagnosticMXBean.class );
        }
        catch ( IOException error )
        {
            throw new RuntimeException( "failed getting Hotspot Diagnostic MX bean", error );
        }
    }

    public static void createHeapDump( String fileName, boolean live )
    {
        try
        {
            HOT_SPOT_DIAGNOSTIC_MX_BEAN.dumpHeap( fileName, live );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}

