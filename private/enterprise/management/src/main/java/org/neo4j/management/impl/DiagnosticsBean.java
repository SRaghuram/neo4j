/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management.impl;

import java.io.StringWriter;
import java.time.ZoneId;
import javax.management.NotCompliantMBeanException;

import org.neo4j.diagnostics.providers.DbmsDiagnosticsManager;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
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
        private final DbmsDiagnosticsManager diagnosticsManager;
        private Config config;

        DiagnosticsImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            config = management.resolveDependency( Config.class );
            this.diagnosticsManager = management.resolveDependency( DbmsDiagnosticsManager.class);
        }

        @Override
        public void dumpToLog()
        {
            diagnosticsManager.dumpAll();
        }

        @Override
        public void dumpDatabaseDiagnosticsToLog( String databaseName )
        {
            diagnosticsManager.dump( databaseName );
        }

        @Override
        public String dumpAll(  )
        {
            StringWriter stringWriter = new StringWriter();
            ZoneId zoneId = config.get( GraphDatabaseSettings.db_timezone ).getZoneId();
            FormattedLog.Builder logBuilder = FormattedLog.withZoneId( zoneId );
            diagnosticsManager.dumpAll( logBuilder.toWriter( stringWriter ) );
            return stringWriter.toString();
        }

        @Override
        public String dumpDatabaseDiagnostics( String databaseName )
        {
            StringWriter stringWriter = new StringWriter();
            ZoneId zoneId = config.get( GraphDatabaseSettings.db_timezone ).getZoneId();
            FormattedLog.Builder logBuilder = FormattedLog.withZoneId( zoneId );
            diagnosticsManager.dump( databaseName, logBuilder.toWriter( stringWriter ) );
            return stringWriter.toString();
        }
    }
}
