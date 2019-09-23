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
package org.neo4j.kernel.impl.transaction.log.entry;

import org.neo4j.storageengine.api.StoreId;

/**
 * Since from 2.2.4 and onwards there's only one version in town, namely {@link LogEntryVersion}.
 */
public class LogVersions
{
    private LogVersions()
    {
        // no instances are allowed
    }

    /**
     * Total 16 bytes
     * - 8 bytes version
     * - 8 bytes last committed tx id
     */
    public static final byte LOG_VERSION_3_5 = 6;

    /**
     * Total 64 bytes
     * - 8 bytes version
     * - 8 bytes last committed tx id
     * - 40 bytes {@link StoreId}
     * - 8 bytes reserved
     */
    public static final byte LOG_VERSION_4_0 = 7;

    public static final byte CURRENT_LOG_FORMAT_VERSION = LOG_VERSION_4_0;
}
