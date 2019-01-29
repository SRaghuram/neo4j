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

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.util.Tuple3;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.ExecutionPlanDescription;

public class PlansSerializer
{
    private static final JsonFactory JSON_FACTORY = new JsonFactory( new Neo4jJsonCodec() );
    private final JsonGenerator out;

    public PlansSerializer( OutputStream output )
    {
        JsonGenerator generator = null;
        try
        {
            generator = JSON_FACTORY.createJsonGenerator( output );
        }
        catch ( IOException e )
        {
            System.out.println( ConcurrentErrorReporter.stackTraceToString( e ) );
        }
        this.out = generator;
    }

    public void serializePlansToJson( List<Tuple3<String,ExecutionPlanDescription,PlanMeta>> planDescriptions )
            throws IOException
    {
        out.writeStartObject();
        try
        {
            for ( Tuple3<String,ExecutionPlanDescription,PlanMeta> planDescriptionAndName : planDescriptions )
            {
                String queryName = planDescriptionAndName._1();
                ExecutionPlanDescription planDescription = planDescriptionAndName._2();
                PlanMeta planMeta = planDescriptionAndName._3();
                if ( null != planDescription )
                {
                    writePlanDescription( queryName, planDescription, planMeta );
                }
                else
                {
                    out.writeFieldName( queryName );
                    writeValue( null );
                }
            }
        }
        finally
        {
            out.writeEndObject();
            out.flush();
        }
    }

    private void writePlanDescription( String queryName, ExecutionPlanDescription planDescription, PlanMeta planMeta )
            throws IOException
    {
        out.writeObjectFieldStart( queryName );
        writePlanMeta( planMeta );
        try
        {
            out.writeObjectFieldStart( "plan" );
            try
            {
                out.writeObjectFieldStart( "root" );
                try
                {
                    writePlanDescriptionObjectBody( planDescription );
                }
                finally
                {
                    out.writeEndObject();
                }
            }
            finally
            {
                out.writeEndObject();
            }
        }
        finally
        {
            out.writeEndObject();
        }
    }

    private void writePlanMeta( PlanMeta planMeta ) throws IOException
    {
        out.writeObjectFieldStart( "planMeta" );
        try
        {
            // Planner Information
            out.writeFieldName( "requestedPlanner" );
            writeValue( planMeta.requestedPlanner() );
            out.writeFieldName( "usedPlanner" );
            writeValue( planMeta.usedPlanner() );
            out.writeFieldName( "defaultPlanner" );
            writeValue( planMeta.defaultPlanner() );
            // cypher statement
            out.writeFieldName( "query" );
            writeValue( planMeta.query() );
            // Plan Compilation Time Information
            out.writeObjectFieldStart( "compileTimeDetail" );
            try
            {
                if ( -1 != planMeta.totalTime() )
                {
                    out.writeFieldName( "totalTime" );
                    writeValue( planMeta.totalTime() );
                }
                if ( -1 != planMeta.parsingTime() )
                {
                    out.writeFieldName( "parsingTime" );
                    writeValue( planMeta.parsingTime() );
                }
                if ( -1 != planMeta.rewritingTime() )
                {
                    out.writeFieldName( "rewritingTime" );
                    writeValue( planMeta.rewritingTime() );
                }
                if ( -1 != planMeta.semanticCheckTime() )
                {
                    out.writeFieldName( "semanticCheckTime" );
                    writeValue( planMeta.semanticCheckTime() );
                }
                if ( -1 != planMeta.planningTime() )
                {
                    out.writeFieldName( "planningTime" );
                    writeValue( planMeta.planningTime() );
                }
                if ( -1 != planMeta.executionPlanBuildingTime() )
                {
                    out.writeFieldName( "executionPlanBuildingTime" );
                    writeValue( planMeta.executionPlanBuildingTime() );
                }
            }
            finally
            {
                out.writeEndObject();
            }
        }
        finally
        {
            out.writeEndObject();
        }
    }

    private void writePlanDescriptionObjectBody( ExecutionPlanDescription planDescription ) throws IOException
    {
        out.writeStringField( "operatorType", planDescription.getName() );
        writePlanArgs( planDescription );

        List<ExecutionPlanDescription> children = planDescription.getChildren();
        out.writeArrayFieldStart( "children" );
        try
        {
            for ( ExecutionPlanDescription child : children )
            {
                out.writeStartObject();
                try
                {
                    writePlanDescriptionObjectBody( child );
                }
                finally
                {
                    out.writeEndObject();
                }
            }
        }
        finally
        {
            out.writeEndArray();
        }
    }

    private void writePlanArgs( ExecutionPlanDescription planDescription ) throws IOException
    {
        for ( Map.Entry<String,Object> entry : planDescription.getArguments().entrySet() )
        {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();

            out.writeFieldName( fieldName );
            writeValue( fieldValue );
        }
    }

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private void writeValue( Object value ) throws IOException
    {
        objectMapper.writeValue( out, value );
    }
}
