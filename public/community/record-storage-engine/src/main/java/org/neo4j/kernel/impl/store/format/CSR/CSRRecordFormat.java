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
package org.neo4j.kernel.impl.store.format.CSR;

import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.format.*;
import org.neo4j.kernel.impl.store.format.aligned.AlignedFormatFamily;
import org.neo4j.kernel.impl.store.format.standard.*;
import org.neo4j.kernel.impl.store.record.*;
import org.neo4j.storageengine.api.IndexCapabilities;

import static org.neo4j.kernel.impl.store.format.StoreVersion.CSR_V1_6_0;

public class CSRRecordFormat extends BaseRecordFormats
{
    public static final RecordFormats RECORD_FORMATS = new CSRRecordFormat();
    public static final String NAME = "CSR";

    private CSRRecordFormat()
    {
        super( CSR_V1_6_0.versionString(), CSR_V1_6_0.introductionVersion(), 1,
                RecordStorageCapability.SCHEMA,
                RecordStorageCapability.DENSE_NODES,
                RecordStorageCapability.POINT_PROPERTIES,
                RecordStorageCapability.TEMPORAL_PROPERTIES,
                RecordStorageCapability.FLEXIBLE_SCHEMA_STORE,
                RecordStorageCapability.INTERNAL_TOKENS,
                RecordStorageCapability.GBPTREE_ID_FILES,
                IndexCapabilities.LuceneCapability.LUCENE_8,
                IndexCapabilities.IndexProviderCapability.INDEX_PROVIDERS_40,
                IndexCapabilities.ConfigCapability.SCHEMA_STORE_CONFIG,
                RecordStorageCapability.GBPTREE_COUNTS_STORE );
    }

    @Override
    public RecordFormat<NodeRecord> node()
    {
        return new CSRNodeRecordFormat( true, neoStores );
    }

    @Override
    public RecordFormat<RelationshipGroupRecord> relationshipGroup()
    {
        return new RelationshipGroupRecordFormat( true );
    }

    @Override
    public RecordFormat<RelationshipRecord> relationship()
    {
        return new CSRRelationshipRecordFormat( true, neoStores );
    }

    @Override
    public RecordFormat<PropertyRecord> property()
    {
        return new PropertyRecordFormat( true );
    }

    @Override
    public RecordFormat<LabelTokenRecord> labelToken()
    {
        return new LabelTokenRecordFormat( true );
    }

    @Override
    public RecordFormat<PropertyKeyTokenRecord> propertyKeyToken()
    {
        return new PropertyKeyTokenRecordFormat( true );
    }

    @Override
    public RecordFormat<RelationshipTypeTokenRecord> relationshipTypeToken()
    {
        return new RelationshipTypeTokenRecordFormat( true );
    }

    @Override
    public RecordFormat<DynamicRecord> dynamic()
    {
        return new DynamicRecordFormat( true );
    }

    @Override
    public RecordFormat<SchemaRecord> schema()
    {
        return new SchemaRecordFormat( true );
    }

    @Override
    public FormatFamily getFormatFamily()
    {
        return AlignedFormatFamily.INSTANCE;
    }

    @Override
    public String name()
    {
        return NAME;
    }

    private NeoStores neoStores;
    @Override
    public RecordFormats setParent(NeoStores neoStores)
    {
        this.neoStores = neoStores;
        return this;
    }

    @Override
    public  NeoStores getParent()
    {
        return neoStores;
    }
}
