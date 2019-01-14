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
package org.neo4j.kernel.api.schema.constraints;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.kernel.api.schema.SchemaUtil;
import org.neo4j.storageengine.api.schema.LabelSchemaDescriptor;

public class NodeKeyConstraintDescriptor extends IndexBackedConstraintDescriptor
{
    NodeKeyConstraintDescriptor( LabelSchemaDescriptor schema )
    {
        super( Type.UNIQUE_EXISTS, schema );
    }

    @Override
    protected String constraintTypeText()
    {
        return "NODE KEY";
    }

    protected String formatProperties( int[] propertyIds, TokenNameLookup tokenNameLookup, String nodeName )
    {
        return SchemaUtil.niceProperties( tokenNameLookup, propertyIds, nodeName + ".", true );
    }
}
