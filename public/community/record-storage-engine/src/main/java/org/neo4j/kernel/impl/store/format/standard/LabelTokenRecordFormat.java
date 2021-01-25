/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.store.format.standard;

import org.neo4j.kernel.impl.store.record.LabelTokenRecord;

public class LabelTokenRecordFormat extends TokenRecordFormat<LabelTokenRecord>
{
    public LabelTokenRecordFormat()
    {
        this( false );
    }

    public LabelTokenRecordFormat( boolean pageAligned )
    {
        super( BASE_RECORD_SIZE, StandardFormatSettings.LABEL_TOKEN_MAXIMUM_ID_BITS, pageAligned );
    }

    @Override
    public LabelTokenRecord newRecord()
    {
        return new LabelTokenRecord( -1 );
    }
}
