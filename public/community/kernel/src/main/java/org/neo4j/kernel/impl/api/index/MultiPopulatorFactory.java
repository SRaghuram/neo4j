/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.api.index;

import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

/**
 * Factory that is able to create {@link BatchingMultipleIndexPopulator}.
 */
public class MultiPopulatorFactory
{
    public MultipleIndexPopulator create( IndexStoreView storeView, LogProvider logProvider, EntityType type, SchemaState schemaState,
            IndexStatisticsStore indexStatisticsStore, JobScheduler jobScheduler, TokenNameLookup tokenNameLookup )
    {
        return new BatchingMultipleIndexPopulator( storeView, logProvider, type, schemaState, indexStatisticsStore, jobScheduler, tokenNameLookup );
    }
}
