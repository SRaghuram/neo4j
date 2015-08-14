/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.consistency.checking.full;

import org.neo4j.consistency.checking.SchemaRecordCheck;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

public class SchemaStoreProcessorTask<R extends AbstractBaseRecord> extends StoreProcessorTask<R>
{
    private final SchemaRecordCheck schemaRecordCheck;

    public SchemaStoreProcessorTask( RecordStore<R> store,
                                     String builderPrefix,
                                     SchemaRecordCheck schemaRecordCheck,
                                     ProgressMonitorFactory.MultiPartBuilder builder,
                                     TaskExecutionOrder order, StoreProcessor singlePassProcessor, StoreProcessor...
            multiPassProcessors )
    {
        super( "SchemaStoreProcessor", store, builderPrefix, builder, order, singlePassProcessor, multiPassProcessors );
        this.schemaRecordCheck = schemaRecordCheck;
    }

    @Override
    protected void beforeProcessing( StoreProcessor processor )
    {
        processor.setSchemaRecordCheck( schemaRecordCheck );
    }

    @Override
    protected void afterProcessing( StoreProcessor processor )
    {
        processor.setSchemaRecordCheck( null );
    }
}
