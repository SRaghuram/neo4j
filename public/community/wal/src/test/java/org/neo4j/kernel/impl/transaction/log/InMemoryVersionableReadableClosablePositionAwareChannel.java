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
package org.neo4j.kernel.impl.transaction.log;

import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_FORMAT_VERSION;

public class InMemoryVersionableReadableClosablePositionAwareChannel extends InMemoryClosableChannel implements
        ReadableLogChannel
{
    public InMemoryVersionableReadableClosablePositionAwareChannel()
    {
        super( true );
    }

    @Override
    public long getVersion()
    {
        return 0;
    }

    @Override
    public byte getLogFormatVersion()
    {
        return CURRENT_LOG_FORMAT_VERSION;
    }
}
