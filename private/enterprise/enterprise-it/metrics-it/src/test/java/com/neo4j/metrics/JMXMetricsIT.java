/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics;

import com.neo4j.configuration.MetricsSettings;
import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.List;
import javax.management.MBeanServer;

import org.neo4j.collection.RawIterator;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.builtin.JmxQueryProcedure;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.api.ResourceTracker.EMPTY_RESOURCE_TRACKER;
import static org.neo4j.values.storable.Values.stringValue;

@EnterpriseDbmsExtension( configurationCallback = "configure" )
class JMXMetricsIT
{
    @Inject
    private TestDirectory directory;
    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private DatabaseManagementService managementService;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( MetricsSettings.metrics_enabled, true );
        builder.setConfig( MetricsSettings.jmx_enabled, true );
        builder.setConfig( MetricsSettings.csv_enabled, false );
        builder.setConfig( preallocate_logical_logs, false );
        builder.setConfig( OnlineBackupSettings.online_backup_enabled, false );
    }

    @Test
    void accessJmxMetricsWhenCsvExporterIsDisabled() throws Exception
    {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        QualifiedName qualifiedName = ProcedureSignature.procedureName( "metricsQuery" );
        JmxQueryProcedure procedure = new JmxQueryProcedure( qualifiedName, mBeanServer );

        TextValue jmxQuery = stringValue( "neo4j.metrics:*" );
        RawIterator<AnyValue[],ProcedureException> result = procedure.apply( null, new AnyValue[]{jmxQuery}, EMPTY_RESOURCE_TRACKER );

        List<AnyValue[]> queryResult = asList( result );
        assertThat( queryResult, hasItem( new MetricsRecordMatcher() ) );
    }

    private static class MetricsRecordMatcher extends TypeSafeMatcher<AnyValue[]>
    {
        @Override
        protected boolean matchesSafely( AnyValue[] item )
        {
            return item.length > 2 && stringValue( "neo4j.metrics:name=neo4j.system.transaction.active_write" ).equals( item[0] ) &&
                    stringValue( "Information on the management interface of the MBean" ).equals( item[1] );
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "Expected to see neo4j.system.transaction.active_write in result set" );
        }
    }
}
