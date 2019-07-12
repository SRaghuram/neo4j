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
package org.neo4j.kernel.impl.transaction.tracing;

public interface DatabaseTracer extends TransactionTracer, CheckPointTracer
{
    DatabaseTracer NULL = new DatabaseTracer()
    {
        @Override
        public long numberOfCheckPoints()
        {
            return 0;
        }

        @Override
        public long checkPointAccumulatedTotalTimeMillis()
        {
            return 0;
        }

        @Override
        public long lastCheckpointTimeMillis()
        {
            return 0;
        }

        @Override
        public LogFileCreateEvent createLogFile()
        {
            return LogFileCreateEvent.NULL;
        }

        @Override
        public LogCheckPointEvent beginCheckPoint()
        {
            return LogCheckPointEvent.NULL;
        }

        @Override
        public TransactionEvent beginTransaction()
        {
            return TransactionEvent.NULL;
        }

        @Override
        public long getAppendedBytes()
        {
            return 0;
        }

        @Override
        public long numberOfLogRotations()
        {
            return 0;
        }

        @Override
        public long logRotationAccumulatedTotalTimeMillis()
        {
            return 0;
        }

        @Override
        public long lastLogRotationTimeMillis()
        {
            return 0;
        }
    };

    LogFileCreateEvent createLogFile();
}
