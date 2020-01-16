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

import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.storageengine.api.CommandCreationContext;

class FrekiCommandCreationContext implements CommandCreationContext
{
    private final IdGenerator nodes;
    private final IdGenerator relationships;
    private final IdGenerator labelTokens;
    private final IdGenerator relationshipTypeTokens;
    private final IdGenerator propertyKeyTokens;
    private final PageCursorTracerSupplier cursorTracerSupplier;

    FrekiCommandCreationContext( IdGeneratorFactory idGeneratorFactory, PageCursorTracerSupplier cursorTracerSupplier )
    {
        nodes = idGeneratorFactory.get( IdType.NODE );
        relationships = idGeneratorFactory.get( IdType.RELATIONSHIP );
        labelTokens = idGeneratorFactory.get( IdType.LABEL_TOKEN );
        relationshipTypeTokens = idGeneratorFactory.get( IdType.RELATIONSHIP_TYPE_TOKEN );
        propertyKeyTokens = idGeneratorFactory.get( IdType.PROPERTY_KEY_TOKEN );
        this.cursorTracerSupplier = cursorTracerSupplier;
    }

    @Override
    public long reserveNode()
    {
        return nodes.nextId( cursorTracerSupplier.get() );
    }

    @Override
    public long reserveRelationship()
    {
        return relationships.nextId( cursorTracerSupplier.get() );
    }

    @Override
    public long reserveSchema()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public int reserveLabelTokenId()
    {
        return (int) labelTokens.nextId( cursorTracerSupplier.get() );
    }

    @Override
    public int reservePropertyKeyTokenId()
    {
        return (int) propertyKeyTokens.nextId( cursorTracerSupplier.get() );
    }

    @Override
    public int reserveRelationshipTypeTokenId()
    {
        return (int) relationshipTypeTokens.nextId( cursorTracerSupplier.get() );
    }

    @Override
    public void close()
    {
    }
}
