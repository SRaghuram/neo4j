/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.index;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.impl.labelscan.LuceneLabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

public class LabelScanStoreLoggingTest
{

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void noLuceneLabelScanStoreMonitorMessages() throws Throwable
    {
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        GraphDatabaseService database = new TestGraphDatabaseFactory()
                        .setInternalLogProvider( logProvider )
                        .newEmbeddedDatabase( testDirectory.directory() );
        try
        {

            NativeLabelScanStore labelScanStore = resolveDependency( (GraphDatabaseAPI) database, NativeLabelScanStore.class);
            performWriteAndRestartStore( labelScanStore );
            logProvider.assertNoLogCallContaining( LuceneLabelScanStore.class.getName() );
            logProvider.assertContainsLogCallContaining( NativeLabelScanStore.class.getName() );
            logProvider.assertContainsMessageContaining(
                    "Scan store recovery completed: Number of cleaned crashed pointers" );
        }
        finally
        {
            database.shutdown();
        }
    }

    @Test
    public void noNativeLabelScanStoreMonitorMessages() throws Throwable
    {
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        GraphDatabaseService database = new TestGraphDatabaseFactory()
                .setInternalLogProvider( logProvider )
                .newEmbeddedDatabaseBuilder( testDirectory.directory() )
                .setConfig( GraphDatabaseSettings.label_index.name(), GraphDatabaseSettings.LabelIndex.LUCENE.name() )
                .newGraphDatabase();
        try
        {
            LuceneLabelScanStore labelScanStore = resolveDependency( (GraphDatabaseAPI) database, LuceneLabelScanStore.class);
            performWriteAndRestartStore( labelScanStore );
            logProvider.assertNoLogCallContaining( NativeLabelScanStore.class.getName() );
            logProvider.assertContainsLogCallContaining( LuceneLabelScanStore.class.getName() );
        }
        finally
        {
            database.shutdown();
        }
    }

    private void performWriteAndRestartStore( LabelScanStore labelScanStore ) throws IOException
    {
        try ( LabelScanWriter labelScanWriter = labelScanStore.newWriter() )
        {
            labelScanWriter.write( NodeLabelUpdate.labelChanges( 1, new long[]{}, new long[]{1} ) );
        }
        labelScanStore.stop();
        labelScanStore.shutdown();

        labelScanStore.init();
        labelScanStore.start();
    }

    private static <T> T resolveDependency( GraphDatabaseAPI database, Class<T> clazz )
    {
        DependencyResolver resolver = database.getDependencyResolver();
        return resolver.resolveDependency( clazz );
    }
}
