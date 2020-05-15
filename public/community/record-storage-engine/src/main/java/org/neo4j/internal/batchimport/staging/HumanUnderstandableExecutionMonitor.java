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
package org.neo4j.internal.batchimport.staging;

import org.neo4j.internal.batchimport.CountGroupsStage;
import org.neo4j.internal.batchimport.DataStatistics;
import org.neo4j.internal.batchimport.NodeDegreeCountStage;
import org.neo4j.internal.batchimport.RelationshipGroupStage;
import org.neo4j.internal.batchimport.ScanAndCacheGroupsStage;
import org.neo4j.internal.batchimport.SparseNodeFirstRelationshipStage;
import org.neo4j.internal.batchimport.cache.NodeRelationshipCache;
import org.neo4j.storageengine.api.BatchingStoreInterface;

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

        } else if ( execution.getStageName().equals( NodeDegreeCountStage.NAME ) )
        {
            endPrevious();

            // Link relationships:
            // - read node degrees
            // - backward linking
            // - node relationship linking
            // - forward linking
            initializeLinking(
                    dependencyResolver.resolveDependency( BatchingStoreInterface.class ),
                    dependencyResolver.resolveDependency( NodeRelationshipCache.class ),
                    dependencyResolver.resolveDependency( DataStatistics.class ) );
        }
        else if ( execution.getStageName().equals( CountGroupsStage.NAME ) )
        {
            endPrevious();

            // Misc:
            // - relationship group defragmentation
            // - counts store
            initializeMisc(
                    dependencyResolver.resolveDependency( BatchingStoreInterface.class ),
                    dependencyResolver.resolveDependency( DataStatistics.class ) );
        }
        else if ( includeStage( execution ) )
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
        if ( includeStage( execution ) )
        {
            updateProgress( progressOf( execution ) );
        }
    }

    private static boolean includeStage( StageExecution execution )
    {
        String name = execution.getStageName();
        return !name.equals( RelationshipGroupStage.NAME ) &&
               !name.equals( SparseNodeFirstRelationshipStage.NAME ) &&
               !name.equals( ScanAndCacheGroupsStage.NAME );
    }
}
