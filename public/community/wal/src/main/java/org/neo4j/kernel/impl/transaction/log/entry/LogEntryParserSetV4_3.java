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
package org.neo4j.kernel.impl.transaction.log.entry;

import org.neo4j.kernel.KernelVersion;

public class LogEntryParserSetV4_3 extends LogEntryParserSetV4_2
{
    LogEntryParserSetV4_3()
    {
        super( KernelVersion.V4_3_D3 );

        // Note: in Neo4j 4.2 checkpoints were separated out from the main transaction log files with the introduction of the "detached" checkpoint log entry.
        // It was also separated into its own LogEntryParserSet and these log entries got a new version. This was unnecessary complexity since even
        // though they would live in separate files they can might as well in the same place code-wise and avoid this complexity. So here we are with:
        // Neo4j version 4.2
        //  - "main" log entry version 2
        //  - "checkpoint" log entry version 3
        // With the version bump in 4.3 and merging the two it looks like this:
        // Neo4j version 4.3
        //  - log entry version 3
        //
        // This means that 4.3 reading the checkpoint log entries file will see entries with version 3, which is the same version as the detached
        // checkpoint log entries had in 4.2 and so will select this parser, which will be able to parse the detached checkpoint.
        //
        // Phew... still with me? This is merely a point of confusion up to this point. From this point on the versioning of all log entries will
        // follow the same scheme, which to some extent means slightly unnecessary version bumps for detached checkpoints if they don't change,
        // but that can be said for other log entries too that won't change between versions. And having them follow the same versioning is so
        // much easier on the brain. Thank you and good day.
        register( new DetachedCheckpointLogEntryParser() );
    }
}
