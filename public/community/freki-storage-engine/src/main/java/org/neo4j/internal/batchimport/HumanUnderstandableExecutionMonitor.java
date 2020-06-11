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

import org.neo4j.internal.batchimport.staging.BaseHumanUnderstandableExecutionMonitor;
import org.neo4j.internal.batchimport.staging.StageExecution;

import static java.lang.System.currentTimeMillis;


/**
 * Prints progress you can actually understand, with capabilities to on demand print completely incomprehensible
 * details only understandable to a select few.
 */
public class HumanUnderstandableExecutionMonitor extends BaseHumanUnderstandableExecutionMonitor
{
    public HumanUnderstandableExecutionMonitor( Monitor monitor )
    {
        super( monitor );
    }

    @Override
    public void start( StageExecution execution )
    {
        // Divide into 4 progress stages:
        if (super.tryStart( execution))
        {

        } else
        {
            stashedProgress += progress;
            progress = 0;
            newInternalStage = true;
        }
        lastReportTime = currentTimeMillis();
    }


    @Override
    public void check( StageExecution execution )
    {
        updateProgress( progressOf( execution ) );
    }

}
