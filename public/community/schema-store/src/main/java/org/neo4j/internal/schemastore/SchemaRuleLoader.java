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
package org.neo4j.internal.schemastore;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.schema.SchemaRule;

import static org.neo4j.internal.schema.SchemaRuleMapifier.unmapifySchemaRule;

class SchemaRuleLoader implements GBPTreeSchemaStore.EntryVisitor<MalformedSchemaRuleException>
{
    private final List<SchemaRule> loadedSchemaRules = new ArrayList<>();
    private SchemaKeyGroup keyGroup = null;

    @Override
    public boolean accept( SchemaKey key, SchemaValue value ) throws MalformedSchemaRuleException
    {
        if ( keyGroup == null || key.id != keyGroup.id )
        {
            loadRuleFromCurrentGroup();
            keyGroup = new SchemaKeyGroup( key.id );
        }
        keyGroup.addValue( key.name(), value.value );
        return false;
    }

    private void loadRuleFromCurrentGroup() throws MalformedSchemaRuleException
    {
        if ( keyGroup != null )
        {
            loadedSchemaRules.add( unmapifySchemaRule( keyGroup.id, keyGroup.values ) );
            keyGroup = null;
        }
    }

    List<SchemaRule> loadedRules() throws MalformedSchemaRuleException
    {
        loadRuleFromCurrentGroup();
        return loadedSchemaRules;
    }
}
