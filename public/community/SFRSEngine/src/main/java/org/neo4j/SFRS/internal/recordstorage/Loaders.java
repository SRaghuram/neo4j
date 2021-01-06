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
package org.neo4j.SFRS.internal.recordstorage;

import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.SFRS.kernel.impl.store.NeoStores;
import org.neo4j.SFRS.kernel.impl.store.PropertyStore;
import org.neo4j.SFRS.kernel.impl.store.RecordStore;
import org.neo4j.SFRS.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.SFRS.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.SFRS.kernel.impl.store.record.NodeRecord;
import org.neo4j.SFRS.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.SFRS.kernel.impl.store.record.PropertyBlock;
import org.neo4j.SFRS.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.SFRS.kernel.impl.store.record.PropertyRecord;
import org.neo4j.SFRS.kernel.impl.store.record.RecordLoad;
import org.neo4j.SFRS.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.SFRS.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.SFRS.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.SFRS.kernel.impl.store.record.SchemaRecord;

import static java.lang.Math.toIntExact;
import static org.neo4j.SFRS.kernel.impl.store.record.RecordLoad.NORMAL;

public class Loaders
{
    private final RecordAccess.Loader<NodeRecord,Void> nodeLoader;
    private final RecordAccess.Loader<PropertyRecord,PrimitiveRecord> propertyLoader;
    private final RecordAccess.Loader<RelationshipRecord,Void> relationshipLoader;
    private final RecordAccess.Loader<RelationshipGroupRecord,Integer> relationshipGroupLoader;
    private final RecordAccess.Loader<SchemaRecord, SchemaRule> schemaRuleLoader;
    private final RecordAccess.Loader<PropertyKeyTokenRecord,Void> propertyKeyTokenLoader;
    private final RecordAccess.Loader<LabelTokenRecord,Void> labelTokenLoader;
    private final RecordAccess.Loader<RelationshipTypeTokenRecord,Void> relationshipTypeTokenLoader;

    public Loaders( NeoStores neoStores )
    {
        this(
                neoStores.getNodeStore(),
                neoStores.getPropertyStore(),
                neoStores.getRelationshipStore(),
                neoStores.getRelationshipGroupStore(),
                neoStores.getPropertyKeyTokenStore(),
                neoStores.getRelationshipTypeTokenStore(),
                neoStores.getLabelTokenStore(),
                neoStores.getSchemaStore() );
    }

    public Loaders(
            RecordStore<NodeRecord> nodeStore,
            PropertyStore propertyStore,
            RecordStore<RelationshipRecord> relationshipStore,
            RecordStore<RelationshipGroupRecord> relationshipGroupStore,
            RecordStore<PropertyKeyTokenRecord> propertyKeyTokenStore,
            RecordStore<RelationshipTypeTokenRecord> relationshipTypeTokenStore,
            RecordStore<LabelTokenRecord> labelTokenStore,
            SchemaStore schemaStore )
    {
        nodeLoader = nodeLoader( nodeStore );
        propertyLoader = propertyLoader( propertyStore );
        relationshipLoader = relationshipLoader( relationshipStore );
        relationshipGroupLoader = relationshipGroupLoader( relationshipGroupStore );
        schemaRuleLoader = schemaRuleLoader( schemaStore );
        propertyKeyTokenLoader = propertyKeyTokenLoader( propertyKeyTokenStore );
        labelTokenLoader = labelTokenLoader( labelTokenStore );
        relationshipTypeTokenLoader = relationshipTypeTokenLoader( relationshipTypeTokenStore );
    }

    public RecordAccess.Loader<NodeRecord,Void> nodeLoader()
    {
        return nodeLoader;
    }

    public RecordAccess.Loader<PropertyRecord,PrimitiveRecord> propertyLoader()
    {
        return propertyLoader;
    }

    public RecordAccess.Loader<RelationshipRecord,Void> relationshipLoader()
    {
        return relationshipLoader;
    }

    public RecordAccess.Loader<RelationshipGroupRecord,Integer> relationshipGroupLoader()
    {
        return relationshipGroupLoader;
    }

    public RecordAccess.Loader<SchemaRecord,SchemaRule> schemaRuleLoader()
    {
        return schemaRuleLoader;
    }

    public RecordAccess.Loader<PropertyKeyTokenRecord,Void> propertyKeyTokenLoader()
    {
        return propertyKeyTokenLoader;
    }

    public RecordAccess.Loader<LabelTokenRecord,Void> labelTokenLoader()
    {
        return labelTokenLoader;
    }

    public RecordAccess.Loader<RelationshipTypeTokenRecord,Void> relationshipTypeTokenLoader()
    {
        return relationshipTypeTokenLoader;
    }

    public static RecordAccess.Loader<NodeRecord,Void> nodeLoader(final RecordStore<NodeRecord> store )
    {
        return new RecordAccess.Loader<>()
        {
            @Override
            public NodeRecord newUnused( long key, Void additionalData )
            {
                return andMarkAsCreated( new NodeRecord( key ) );
            }

            @Override
            public NodeRecord load( long key, Void additionalData, PageCursorTracer cursorTracer )
            {
                return store.getRecord( key, store.newRecord(), NORMAL, cursorTracer );
            }

            @Override
            public void ensureHeavy( NodeRecord record, PageCursorTracer cursorTracer )
            {
                store.ensureHeavy( record, cursorTracer );
            }

            @Override
            public NodeRecord copy( NodeRecord nodeRecord )
            {
                return new NodeRecord( nodeRecord );
            }
        };
    }

    public static RecordAccess.Loader<PropertyRecord,PrimitiveRecord> propertyLoader(final PropertyStore store )
    {
        return new RecordAccess.Loader<>()
        {
            @Override
            public PropertyRecord newUnused( long key, PrimitiveRecord additionalData )
            {
                PropertyRecord record = new PropertyRecord( key );
                setOwner( record, additionalData );
                return andMarkAsCreated( record );
            }

            private void setOwner( PropertyRecord record, PrimitiveRecord owner )
            {
                if ( owner != null )
                {
                    owner.setIdTo( record );
                }
            }

            @Override
            public PropertyRecord load( long key, PrimitiveRecord additionalData, PageCursorTracer cursorTracer )
            {
                PropertyRecord record = store.getRecord( key, store.newRecord(), NORMAL, cursorTracer );
                setOwner( record, additionalData );
                return record;
            }

            @Override
            public void ensureHeavy( PropertyRecord record, PageCursorTracer cursorTracer )
            {
                for ( PropertyBlock block : record )
                {
                    store.ensureHeavy( block, cursorTracer );
                }
            }

            @Override
            public PropertyRecord copy( PropertyRecord propertyRecord )
            {
                return new PropertyRecord( propertyRecord );
            }
        };
    }

    public static RecordAccess.Loader<RelationshipRecord,Void> relationshipLoader(
            final RecordStore<RelationshipRecord> store )
    {
        return new RecordAccess.Loader<>()
        {
            @Override
            public RelationshipRecord newUnused( long key, Void additionalData )
            {
                return andMarkAsCreated( new RelationshipRecord( key ) );
            }

            @Override
            public RelationshipRecord load( long key, Void additionalData, PageCursorTracer cursorTracer )
            {
                return store.getRecord( key, store.newRecord(), NORMAL, cursorTracer );
            }

            @Override
            public void ensureHeavy( RelationshipRecord record, PageCursorTracer cursorTracer )
            {   // Nothing to load
            }

            @Override
            public RelationshipRecord copy( RelationshipRecord relationshipRecord )
            {
                return new RelationshipRecord( relationshipRecord );
            }
        };
    }

    public static RecordAccess.Loader<RelationshipGroupRecord,Integer> relationshipGroupLoader(
            final RecordStore<RelationshipGroupRecord> store )
    {
        return new RecordAccess.Loader<>()
        {
            @Override
            public RelationshipGroupRecord newUnused( long key, Integer type )
            {
                RelationshipGroupRecord record = new RelationshipGroupRecord( key );
                record.setType( type );
                return andMarkAsCreated( record );
            }

            @Override
            public RelationshipGroupRecord load( long key, Integer type, PageCursorTracer cursorTracer )
            {
                return store.getRecord( key, store.newRecord(), NORMAL, cursorTracer );
            }

            @Override
            public void ensureHeavy( RelationshipGroupRecord record, PageCursorTracer cursorTracer )
            {   // Not needed
            }

            @Override
            public RelationshipGroupRecord copy( RelationshipGroupRecord record )
            {
                return new RelationshipGroupRecord( record );
            }
        };
    }

    private static RecordAccess.Loader<SchemaRecord, SchemaRule> schemaRuleLoader(final SchemaStore store )
    {
        return new RecordAccess.Loader<>()
        {
            @Override
            public SchemaRecord newUnused( long key, SchemaRule additionalData )
            {
                return andMarkAsCreated( new SchemaRecord( key ) );
            }

            @Override
            public SchemaRecord load( long key, SchemaRule additionalData, PageCursorTracer cursorTracer )
            {
                return store.getRecord( key, store.newRecord(), RecordLoad.NORMAL, cursorTracer );
            }

            @Override
            public void ensureHeavy( SchemaRecord record, PageCursorTracer cursorTracer )
            {
            }

            @Override
            public SchemaRecord copy( SchemaRecord record )
            {
                return new SchemaRecord( record );
            }
        };
    }

    public static RecordAccess.Loader<PropertyKeyTokenRecord,Void> propertyKeyTokenLoader(
            final RecordStore<PropertyKeyTokenRecord> store )
    {
        return new RecordAccess.Loader<>()
        {
            @Override
            public PropertyKeyTokenRecord newUnused( long key, Void additionalData )
            {
                return andMarkAsCreated( new PropertyKeyTokenRecord( toIntExact( key ) ) );
            }

            @Override
            public PropertyKeyTokenRecord load( long key, Void additionalData, PageCursorTracer cursorTracer )
            {
                return store.getRecord( key, store.newRecord(), NORMAL, cursorTracer );
            }

            @Override
            public void ensureHeavy( PropertyKeyTokenRecord record, PageCursorTracer cursorTracer )
            {
                store.ensureHeavy( record, cursorTracer );
            }

            @Override
            public PropertyKeyTokenRecord copy( PropertyKeyTokenRecord record )
            {
                return new PropertyKeyTokenRecord( record );
            }
        };
    }

    public static RecordAccess.Loader<LabelTokenRecord,Void> labelTokenLoader(
            final RecordStore<LabelTokenRecord> store )
    {
        return new RecordAccess.Loader<>()
        {
            @Override
            public LabelTokenRecord newUnused( long key, Void additionalData )
            {
                return andMarkAsCreated( new LabelTokenRecord( toIntExact( key ) ) );
            }

            @Override
            public LabelTokenRecord load( long key, Void additionalData, PageCursorTracer cursorTracer )
            {
                return store.getRecord( key, store.newRecord(), NORMAL, cursorTracer );
            }

            @Override
            public void ensureHeavy( LabelTokenRecord record, PageCursorTracer cursorTracer )
            {
                store.ensureHeavy( record, cursorTracer );
            }

            @Override
            public LabelTokenRecord copy( LabelTokenRecord record )
            {
                return new LabelTokenRecord( record );
            }
        };
    }

    public static RecordAccess.Loader<RelationshipTypeTokenRecord,Void> relationshipTypeTokenLoader(
            final RecordStore<RelationshipTypeTokenRecord> store )
    {
        return new RecordAccess.Loader<>()
        {
            @Override
            public RelationshipTypeTokenRecord newUnused( long key, Void additionalData )
            {
                return andMarkAsCreated( new RelationshipTypeTokenRecord( toIntExact( key ) ) );
            }

            @Override
            public RelationshipTypeTokenRecord load( long key, Void additionalData, PageCursorTracer cursorTracer )
            {
                return store.getRecord( key, store.newRecord(), NORMAL, cursorTracer );
            }

            @Override
            public void ensureHeavy( RelationshipTypeTokenRecord record, PageCursorTracer cursorTracer )
            {
                store.ensureHeavy( record, cursorTracer );
            }

            @Override
            public RelationshipTypeTokenRecord copy( RelationshipTypeTokenRecord record )
            {
                return new RelationshipTypeTokenRecord( record );
            }
        };
    }

    protected static <RECORD extends AbstractBaseRecord> RECORD andMarkAsCreated( RECORD record )
    {
        record.setCreated();
        return record;
    }
}
