/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management.impl;

import java.io.StringWriter;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import javax.management.NotCompliantMBeanException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.internal.diagnostics.DiagnosticsManager;
import org.neo4j.internal.diagnostics.DiagnosticsProvider;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.FormattedLog;
import org.neo4j.management.Diagnostics;

@Service.Implementation( ManagementBeanProvider.class )
public class DiagnosticsBean extends ManagementBeanProvider
{
    public DiagnosticsBean()
    {
        super( Diagnostics.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new DiagnosticsImpl( management );
    }

    private static class DiagnosticsImpl extends Neo4jMBean implements Diagnostics
    {
        private final DiagnosticsManager diagnostics;
        private Config config;

        DiagnosticsImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            config = management.resolveDependency( Config.class );
            this.diagnostics = management.resolveDependency( DiagnosticsManager.class);
        }

        @Override
        public void dumpToLog()
        {
            diagnostics.dumpAll();
        }

        @Override
        public List<String> getDiagnosticsProviders()
        {
            List<String> result = new ArrayList<>();
            for ( DiagnosticsProvider provider : diagnostics )
            {
                result.add( provider.getDiagnosticsIdentifier() );
            }
            return result;
        }

        @Override
        public void dumpToLog( String providerId )
        {
            diagnostics.dump( providerId );
        }

        @Override
        public String dumpAll(  )
        {
            StringWriter stringWriter = new StringWriter();
            ZoneId zoneId = config.get( GraphDatabaseSettings.db_timezone ).getZoneId();
            FormattedLog.Builder logBuilder = FormattedLog.withZoneId( zoneId );
            diagnostics.dumpAll( logBuilder.toWriter( stringWriter ) );
            return stringWriter.toString();
        }

        @Override
        public String extract( String providerId )
        {
            StringWriter stringWriter = new StringWriter();
            ZoneId zoneId = config.get( GraphDatabaseSettings.db_timezone ).getZoneId();
            FormattedLog.Builder logBuilder = FormattedLog.withZoneId( zoneId );
            diagnostics.extract( providerId, logBuilder.toWriter( stringWriter ) );
            return stringWriter.toString();
        }
    }
}
