package org.neo4j.internal.batchimport;

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

import org.neo4j.internal.batchimport.staging.BaseExecutionMonitors;
import org.neo4j.internal.batchimport.staging.BaseHumanUnderstandableExecutionMonitor;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;

import static org.neo4j.internal.batchimport.staging.BaseHumanUnderstandableExecutionMonitor.NO_MONITOR;

/**
 * Common {@link ExecutionMonitor} implementations.
 */
public class FrekiExecutionMonitors extends BaseExecutionMonitors
{
    private FrekiExecutionMonitors()
    {
        super();
        throw new AssertionError( "No instances allowed" );
    }

    public static ExecutionMonitor defaultVisible()
    {
        return new HumanUnderstandableExecutionMonitor(NO_MONITOR);
    }
}
