/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.store;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;

/**
 * Not thread safe (since DiffRecordStore is not thread safe), intended for
 * single threaded use.
 *
 * Make sure to call {@link #initialize()} after constructor has been run.
 */
public class StoreAccess
{
    // Top level stores
    private SchemaStore schemaStore;
    private RecordStore<NodeRecord> nodeStore;
    private RecordStore<RelationshipRecord> relStore;
    private RecordStore<RelationshipTypeTokenRecord> relationshipTypeTokenStore;
    private RecordStore<LabelTokenRecord> labelTokenStore;
    private RecordStore<DynamicRecord> nodeDynamicLabelStore;
    private RecordStore<PropertyRecord> propStore;
    // Transitive stores
    private RecordStore<DynamicRecord> stringStore;
    private RecordStore<DynamicRecord> arrayStore;
    private RecordStore<PropertyKeyTokenRecord> propertyKeyTokenStore;
    private RecordStore<DynamicRecord> relationshipTypeNameStore;
    private RecordStore<DynamicRecord> labelNameStore;
    private RecordStore<DynamicRecord> propertyKeyNameStore;
    private RecordStore<RelationshipGroupRecord> relGroupStore;
    // internal state
    private boolean closeable;
    private final NeoStores neoStores;
    private List<IdGenerator> idGenerators = new ArrayList<>();

    public StoreAccess( NeoStores store )
    {
        this.neoStores = store;
    }

    public StoreAccess( FileSystemAbstraction fileSystem, PageCache pageCache, DatabaseLayout directoryStructure, Config config )
    {
        this( new StoreFactory( directoryStructure, config, new DefaultIdGeneratorFactory( fileSystem, immediate() ), pageCache,
                fileSystem, NullLogProvider.getInstance() ).openAllNeoStores() );
        this.closeable = true;
    }

    /**
     * This method exists since {@link #wrapStore(RecordStore)} might depend on the existence of a variable
     * that gets set in a subclass' constructor <strong>after</strong> this constructor of {@link StoreAccess}
     * has been executed. I.e. a correct creation of a {@link StoreAccess} instance must be the creation of the
     * object plus a call to this method.
     *
     * @return this
     */
    public StoreAccess initialize()
    {
        // All stores
        final SchemaStore schemaStore = neoStores.getSchemaStore();
        final NodeStore nodeStore = neoStores.getNodeStore();
        final RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        final PropertyStore propertyStore = neoStores.getPropertyStore();
        final DynamicStringStore propertyStringStore = propertyStore.getStringStore();
        final DynamicArrayStore propertyArrayStore = propertyStore.getArrayStore();
        final RelationshipTypeTokenStore relationshipTypeTokenStore = neoStores.getRelationshipTypeTokenStore();
        final LabelTokenStore labelTokenStore = neoStores.getLabelTokenStore();
        final DynamicArrayStore dynamicLabelStore = nodeStore.getDynamicLabelStore();
        final PropertyKeyTokenStore propertyKeyTokenStore = propertyStore.getPropertyKeyTokenStore();
        final DynamicStringStore relationshipTypeTokenNameStore = relationshipTypeTokenStore.getNameStore();
        final DynamicStringStore labelTokenNameStore = labelTokenStore.getNameStore();
        final DynamicStringStore propertyTypeTokenNameStore = propertyKeyTokenStore.getNameStore();
        final RelationshipGroupStore relationshipGroupStore = neoStores.getRelationshipGroupStore();
        final MetaDataStore metaDataStore = neoStores.getMetaDataStore();

        // Extract idGenerators
        extractIdGenerators(
                schemaStore,
                nodeStore,
                relationshipStore,
                propertyStore,
                propertyStringStore,
                propertyArrayStore,
                relationshipTypeTokenStore,
                labelTokenStore,
                dynamicLabelStore,
                propertyKeyTokenStore,
                relationshipTypeTokenNameStore,
                labelTokenNameStore,
                propertyTypeTokenNameStore,
                relationshipGroupStore,
                metaDataStore );

        // Wrap stores and assign to fields
        this.schemaStore = schemaStore;
        this.nodeStore = wrapStore( nodeStore );
        this.relStore = wrapStore( relationshipStore );
        this.propStore = wrapStore( propertyStore );
        this.stringStore = wrapStore( propertyStringStore );
        this.arrayStore = wrapStore( propertyArrayStore );
        this.relationshipTypeTokenStore = wrapStore( relationshipTypeTokenStore );
        this.labelTokenStore = wrapStore( labelTokenStore );
        this.nodeDynamicLabelStore = wrapStore( wrapNodeDynamicLabelStore( dynamicLabelStore ) );
        this.propertyKeyTokenStore = wrapStore( propertyKeyTokenStore );
        this.relationshipTypeNameStore = wrapStore( relationshipTypeTokenNameStore );
        this.labelNameStore = wrapStore( labelTokenNameStore );
        this.propertyKeyNameStore = wrapStore( propertyTypeTokenNameStore );
        this.relGroupStore = wrapStore( relationshipGroupStore );

        return this;
    }

    private void extractIdGenerators( CommonAbstractStore... commonAbstractStores )
    {
        for ( CommonAbstractStore commonAbstractStore : commonAbstractStores )
        {
            idGenerators.add( commonAbstractStore.getIdGenerator() );
        }
    }

    public NeoStores getRawNeoStores()
    {
        return neoStores;
    }

    public SchemaStore getSchemaStore()
    {
        return schemaStore;
    }

    public RecordStore<NodeRecord> getNodeStore()
    {
        return nodeStore;
    }

    public RecordStore<RelationshipRecord> getRelationshipStore()
    {
        return relStore;
    }

    public RecordStore<RelationshipGroupRecord> getRelationshipGroupStore()
    {
        return relGroupStore;
    }

    public RecordStore<PropertyRecord> getPropertyStore()
    {
        return propStore;
    }

    public RecordStore<DynamicRecord> getStringStore()
    {
        return stringStore;
    }

    public RecordStore<DynamicRecord> getArrayStore()
    {
        return arrayStore;
    }

    public RecordStore<RelationshipTypeTokenRecord> getRelationshipTypeTokenStore()
    {
        return relationshipTypeTokenStore;
    }

    public RecordStore<LabelTokenRecord> getLabelTokenStore()
    {
        return labelTokenStore;
    }

    public RecordStore<DynamicRecord> getNodeDynamicLabelStore()
    {
        return nodeDynamicLabelStore;
    }

    public RecordStore<PropertyKeyTokenRecord> getPropertyKeyTokenStore()
    {
        return propertyKeyTokenStore;
    }

    public RecordStore<DynamicRecord> getRelationshipTypeNameStore()
    {
        return relationshipTypeNameStore;
    }

    public RecordStore<DynamicRecord> getLabelNameStore()
    {
        return labelNameStore;
    }

    public RecordStore<DynamicRecord> getPropertyKeyNameStore()
    {
        return propertyKeyNameStore;
    }

    public List<IdGenerator> idGenerators()
    {
        return idGenerators;
    }

    private static RecordStore<DynamicRecord> wrapNodeDynamicLabelStore( RecordStore<DynamicRecord> store )
    {
        return new RecordStore.Delegator<>( store )
        {
            @Override
            public <FAILURE extends Exception> void accept( Processor<FAILURE> processor, DynamicRecord record )
                    throws FAILURE
            {
                processor.processLabelArrayWithOwner( this, record );
            }
        };
    }

    protected <R extends AbstractBaseRecord> RecordStore<R> wrapStore( RecordStore<R> store )
    {
        return store;
    }

    @SuppressWarnings( "unchecked" )
    protected <FAILURE extends Exception> void apply( RecordStore.Processor<FAILURE> processor, RecordStore<?> store )
            throws FAILURE
    {
        processor.applyFiltered( store );
    }

    public synchronized void close()
    {
        if ( closeable )
        {
            closeable = false;
            neoStores.close();
        }
    }
}
