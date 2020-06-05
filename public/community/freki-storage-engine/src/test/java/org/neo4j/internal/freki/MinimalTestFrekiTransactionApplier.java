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
package org.neo4j.internal.freki;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Consumer;

import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.util.IdUpdateListener;

import static java.util.Collections.singletonList;
import static org.neo4j.internal.freki.FrekiTransactionApplier.writeBigValue;
import static org.neo4j.internal.freki.FrekiTransactionApplier.writeDenseNode;
import static org.neo4j.internal.freki.FrekiTransactionApplier.writeSparseNode;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

class MinimalTestFrekiTransactionApplier implements Consumer<StorageCommand>
{
    private final MainStores stores;
    private final FrekiCommand.Dispatcher monitor;

    MinimalTestFrekiTransactionApplier( MainStores stores, FrekiCommand.Dispatcher monitor )
    {
        this.stores = stores;
        this.monitor = monitor;
    }

    @Override
    public void accept( StorageCommand command )
    {
        try
        {
            if ( command instanceof FrekiCommand.SparseNode )
            {
                FrekiCommand.SparseNode sparseNode = (FrekiCommand.SparseNode) command;
                monitor.handle( sparseNode );
                writeSparseNode( sparseNode, stores, null, NULL );
            }
            else if ( command instanceof FrekiCommand.DenseNode )
            {
                FrekiCommand.DenseNode denseNode = (FrekiCommand.DenseNode) command;
                monitor.handle( denseNode );
                writeDenseNode( singletonList( denseNode ), stores.denseStore, NULL );
            }
            else if ( command instanceof FrekiCommand.BigPropertyValue )
            {
                FrekiCommand.BigPropertyValue bigValue = (FrekiCommand.BigPropertyValue) command;
                monitor.handle( bigValue );
                writeBigValue( bigValue, stores.bigPropertyValueStore, IdUpdateListener.DIRECT, NULL );
            }
            else
            {
                throw new UnsupportedOperationException( "Cannot yet handle " + command );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    static final FrekiCommand.Dispatcher NO_MONITOR = new FrekiCommand.Dispatcher.Adapter();
}
