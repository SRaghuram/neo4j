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
package org.neo4j.SFRS.internal.batchimport.staging;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.internal.batchimport.Configuration;

/**
 * A stage of processing, mainly consisting of one or more {@link Step steps} that batches of data to
 * process flows through.
 */
public class Stage
{
    private final List<Step<?>> pipeline = new ArrayList<>();
    private final StageExecution execution;

    public Stage( String name, String part, Configuration config, int orderingGuarantees )
    {
        this.execution = new StageExecution( name, part, config, pipeline, orderingGuarantees );
    }

    protected StageControl control()
    {
        return execution;
    }

    public void add( Step<?> step )
    {
        pipeline.add( step );
    }

    public StageExecution execute()
    {
        linkSteps();
        execution.start();
        pipeline.get( 0 ).receive( 1 /*a ticket, ignored anyway*/, null /*serves only as a start signal anyway*/ );
        return execution;
    }

    private void linkSteps()
    {
        Step<?> previous = null;
        for ( Step<?> step : pipeline )
        {
            if ( previous != null )
            {
                previous.setDownstream( step );
            }
            previous = step;
        }
    }

    public void close()
    {
        Exception exception = null;
        for ( Step<?> step : pipeline )
        {
            try
            {
                step.close();
            }
            catch ( Exception e )
            {
                if ( exception == null )
                {
                    exception = e;
                }
                else
                {
                    exception.addSuppressed( e );
                }
            }
        }
        execution.close();
        if ( exception != null )
        {
            throw new RuntimeException( exception );
        }
    }

    @Override
    public String toString()
    {
        return execution.getStageName();
    }
}
