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
package org.neo4j.kernel.impl.api.index;

import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.index.SchemaIndexProvider.Descriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;

/**
 * Small class for holding a bunch of information about an index that we need to rebuild during recovery.
 */
public class RebuildingIndexDescriptor
{
    private final IndexDescriptor indexDescriptor;
    private final Descriptor providerDescriptor;

    RebuildingIndexDescriptor( IndexDescriptor indexDescriptor, SchemaIndexProvider.Descriptor providerDescriptor )
    {
        this.indexDescriptor = indexDescriptor;
        this.providerDescriptor = providerDescriptor;
    }

    public IndexDescriptor getIndexDescriptor()
    {
        return indexDescriptor;
    }

    public Descriptor getProviderDescriptor()
    {
        return providerDescriptor;
    }
}
