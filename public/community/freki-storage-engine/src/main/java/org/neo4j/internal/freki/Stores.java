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

import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.schema.SchemaCache;
import org.neo4j.internal.schemastore.GBPTreeSchemaStore;
import org.neo4j.internal.tokenstore.GBPTreeTokenStore;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.TransactionMetaDataStore;

class Stores extends MainStores
{
    static final String META_DATA_STORE_FILENAME = "meta-data-store";

    private final IdGenerator relationshipIdGenerator;
    final TransactionMetaDataStore metaDataStore;
    final GBPTreeCountsStore countsStore;
    final GBPTreeSchemaStore schemaStore;
    final SchemaCache schemaCache;
    final GBPTreeTokenStore propertyKeyTokenStore;
    final GBPTreeTokenStore relationshipTypeTokenStore;
    final GBPTreeTokenStore labelTokenStore;

    Stores( SimpleStore[] mainStores, BigPropertyValueStore bigPropertyValueStore, DenseStore denseStore, IdGenerator relationshipIdGenerator,
            TransactionMetaDataStore metaDataStore, GBPTreeCountsStore countsStore, GBPTreeSchemaStore schemaStore, SchemaCache schemaCache,
            GBPTreeTokenStore propertyKeyTokenStore, GBPTreeTokenStore relationshipTypeTokenStore, GBPTreeTokenStore labelTokenStore )
    {
        super( mainStores, bigPropertyValueStore, denseStore );
        this.relationshipIdGenerator = relationshipIdGenerator;
        this.metaDataStore = metaDataStore;
        this.countsStore = countsStore;
        this.schemaStore = schemaStore;
        this.schemaCache = schemaCache;
        this.propertyKeyTokenStore = propertyKeyTokenStore;
        this.relationshipTypeTokenStore = relationshipTypeTokenStore;
        this.labelTokenStore = labelTokenStore;
        life.add( onShutdown( relationshipIdGenerator::close ) );
        life.add( onShutdown( metaDataStore::close ) );
        life.add( onShutdown( countsStore::close ) );
        life.add( onShutdown( schemaStore::close ) );
        life.add( onShutdown( propertyKeyTokenStore::close ) );
        life.add( onShutdown( relationshipTypeTokenStore::close ) );
        life.add( onShutdown( labelTokenStore::close ) );
    }

    @Override
    void flushAndForce( IOLimiter limiter, PageCursorTracer cursorTracer )
    {
        super.flushAndForce( limiter, cursorTracer );
        relationshipIdGenerator.checkpoint( limiter, cursorTracer );
        metaDataStore.flush( cursorTracer );
        schemaStore.checkpoint( limiter, cursorTracer );
        propertyKeyTokenStore.checkpoint( limiter, cursorTracer );
        relationshipTypeTokenStore.checkpoint( limiter, cursorTracer );
        labelTokenStore.checkpoint( limiter, cursorTracer );
    }
}
