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
package org.neo4j.internal.batchimport;

import org.neo4j.internal.batchimport.staging.BatchSender;
import org.neo4j.internal.batchimport.staging.ProcessorStep;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.index.label.LabelScanWriter;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;
import static org.neo4j.kernel.impl.store.NodeLabelsField.get;
import static org.neo4j.storageengine.api.NodeLabelUpdate.labelChanges;

public class LabelIndexWriterStep extends ProcessorStep<NodeRecord[]>
{
    private final LabelScanWriter writer;
    private final NodeStore nodeStore;

    public LabelIndexWriterStep( StageControl control, Configuration config, LabelScanStore store,
            NodeStore nodeStore )
    {
        super( control, "LABEL INDEX", config, 1 );
        this.writer = store.newBulkAppendWriter();
        this.nodeStore = nodeStore;
    }

    @Override
    protected void process( NodeRecord[] batch, BatchSender sender ) throws Throwable
    {
        for ( NodeRecord node : batch )
        {
            if ( node.inUse() )
            {
                writer.write( labelChanges( node.getId(), EMPTY_LONG_ARRAY, get( node, nodeStore, TRACER_SUPPLIER.get() ) ) );
            }
        }
        sender.send( batch );
    }

    @Override
    public void close() throws Exception
    {
        super.close();
        writer.close();
    }
}
