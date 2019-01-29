/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
 */

package com.neo4j.bench.ldbc.utils;

import org.neo4j.graphdb.ExecutionPlanDescription;

public class PlanMeta
{
    // planner
    private String requestedPlanner;
    private String usedPlanner;
    private String defaultPlanner;
    // planning time details
    private long totalTime = -1;
    private long parsingTime = -1;
    private long rewritingTime = -1;
    private long semanticCheckTime = -1;
    private long planningTime = -1;
    private long executionPlanBuildingTime = -1;
    // cypher statement
    private String query;

    // Planners

    public String requestedPlanner()
    {
        return requestedPlanner;
    }

    public String usedPlanner()
    {
        return usedPlanner;
    }

    public String defaultPlanner()
    {
        return defaultPlanner;
    }

    public void setRequestedPlanner( String requestedPlanner )
    {
        this.requestedPlanner = requestedPlanner;
    }

    public void setUsedPlanner( String usedPlanner )
    {
        this.usedPlanner = usedPlanner;
    }

    public void setDefaultPlanner( String defaultPlanner )
    {
        this.defaultPlanner = defaultPlanner;
    }

    // Cypher Statement

    public String query()
    {
        return query;
    }

    public void setQuery( String query )
    {
        this.query = query;
    }

    // Plan Compilation Time

    public long totalTime()
    {
        return totalTime;
    }

    public long parsingTime()
    {
        return parsingTime;
    }

    public long rewritingTime()
    {
        return rewritingTime;
    }

    public long semanticCheckTime()
    {
        return semanticCheckTime;
    }

    public long planningTime()
    {
        return planningTime;
    }

    public long executionPlanBuildingTime()
    {
        return executionPlanBuildingTime;
    }

    public void setTotalTime( long totalTime )
    {
        this.totalTime = totalTime;
    }

    public void setParsingTime( long parsingTime )
    {
        this.parsingTime = parsingTime;
    }

    public void setRewritingTime( long rewritingTime )
    {
        this.rewritingTime = rewritingTime;
    }

    public void setSemanticCheckTime( long semanticCheckTime )
    {
        this.semanticCheckTime = semanticCheckTime;
    }

    public void setPlanningTime( long planningTime )
    {
        this.planningTime = planningTime;
    }

    public void setExecutionPlanBuildingTime( long executionPlanBuildingTime )
    {
        this.executionPlanBuildingTime = executionPlanBuildingTime;
    }

    public static String extractPlanner( ExecutionPlanDescription planDescription )
    {
        return ((String) planDescription.getArguments().get( "planner" )).toLowerCase();
    }

    @Override
    public String toString()
    {
        return "PlanMeta{" +
               "requestedPlanner='" + requestedPlanner + '\'' +
               ", usedPlanner='" + usedPlanner + '\'' +
               ", defaultPlanner='" + defaultPlanner + '\'' +
               ", totalTime=" + totalTime +
               ", parsingTime=" + parsingTime +
               ", rewritingTime=" + rewritingTime +
               ", semanticCheckTime=" + semanticCheckTime +
               ", planningTime=" + planningTime +
               ", executionPlanBuildingTime=" + executionPlanBuildingTime +
               ", query='" + query + '\'' +
               '}';
    }
}
