/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.api.impl.schema;

import java.nio.file.Path;

import org.neo4j.annotations.documented.ReporterFactories;
import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexProviderCompatibilityTestSuite;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.schema.ConsistencyCheckable;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProviderFactory;
import org.neo4j.monitoring.Monitors;

import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10;
import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

public class GenericIndexProviderCompatibilitySuiteTest extends IndexProviderCompatibilityTestSuite
{
    @Override
    protected IndexProvider createIndexProvider( PageCache pageCache, FileSystemAbstraction fs, Path graphDbDir )
    {
        Monitors monitors = new Monitors();
        String monitorTag = "";
        Config config = Config.defaults( default_schema_provider, NATIVE_BTREE10.providerName() );
        OperationalMode mode = OperationalMode.SINGLE;
        RecoveryCleanupWorkCollector recoveryCleanupWorkCollector = RecoveryCleanupWorkCollector.immediate();
        return GenericNativeIndexProviderFactory.
                create( pageCache, graphDbDir, fs, monitors, monitorTag, config, mode, recoveryCleanupWorkCollector );
    }

    @Override
    public boolean supportsSpatial()
    {
        return true;
    }

    @Override
    public boolean supportsGranularCompositeQueries()
    {
        return true;
    }

    @Override
    public boolean supportsBooleanRangeQueries()
    {
        return true;
    }

    @Override
    public void consistencyCheck( IndexPopulator populator )
    {
        ((ConsistencyCheckable) populator).consistencyCheck( ReporterFactories.throwingReporterFactory(), NULL );
    }
}
