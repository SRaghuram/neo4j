/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures;

import com.neo4j.dbms.QuarantineOperator;

import java.time.Clock;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;

import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.builtin.ProceduresTimeFormatHelper;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.kernel.api.exceptions.Status.Procedure.ProcedureCallFailed;

public class QuarantineProcedure extends CallableProcedure.BasicProcedure
{
    private static final String PROCEDURE_NAME = "quarantineDatabase";
    private static final String[] PROCEDURE_NAMESPACE = {"dbms", "cluster"};
    private static final String PARAMETER_DATABASE_NAME = "databaseName";
    private static final String PARAMETER_STATE = "setStatus";
    private static final String PARAMETER_REASON = "reason";

    private final QuarantineOperator quarantineOperator;
    private final Clock clock;
    private final ZoneId zoneId;

    public QuarantineProcedure( QuarantineOperator quarantineOperator, Clock clock, ZoneId zoneId )
    {
        super( procedureSignature( new QualifiedName( PROCEDURE_NAMESPACE, PROCEDURE_NAME ) )
                .in( PARAMETER_DATABASE_NAME, Neo4jTypes.NTString )
                .in( PARAMETER_STATE, Neo4jTypes.NTBoolean )
                .in( PARAMETER_REASON, Neo4jTypes.NTString, new DefaultParameterValue( "No reason given", Neo4jTypes.NTString ) )
                .out( "databaseName", Neo4jTypes.NTString )
                .out( "quarantined", Neo4jTypes.NTBoolean )
                .out( "result", Neo4jTypes.NTString )
                .description( "Place a database into quarantine or remove from it." )
                .systemProcedure()
                .admin( true )
                .mode( Mode.DBMS )
                .build() );
        this.quarantineOperator = quarantineOperator;
        this.clock = clock;
        this.zoneId = zoneId;
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> apply( Context ctx, AnyValue[] input, ResourceTracker resourceTracker ) throws ProcedureException
    {
        var databaseName = extractParameter( input, 0, PARAMETER_DATABASE_NAME, TextValue.class ).stringValue();
        var setStatus = extractParameter( input, 1, PARAMETER_STATE, BooleanValue.class ).booleanValue();

        ctx.securityContext().assertCredentialsNotExpired();
        if ( !ctx.procedureCallContext().isSystemDatabase() )
        {
            throw new ProcedureException( ProcedureCallFailed,
                    "This is an administration command and it should be executed against the system database: dbms.quarantineDatabase" );
        }

        try
        {
            String result;
            if ( setStatus )
            {
                var reason = extractParameter( input, 2, PARAMETER_REASON, TextValue.class ).stringValue();
                var user = ctx.securityContext().subject().username();
                var time = ProceduresTimeFormatHelper.formatTime( clock.instant(), zoneId );
                var message = String.format( "By %s at %s: %s", user, time, reason );
                result = quarantineOperator.putIntoQuarantine( databaseName, message );
            }
            else
            {
                result = quarantineOperator.removeFromQuarantine( databaseName );
            }

            var resultRows = new ArrayList<AnyValue[]>();
            resultRows.add( new AnyValue[]{Values.stringValue( databaseName ), Values.booleanValue( setStatus ), Values.stringValue( result )} );
            return RawIterator.wrap( resultRows.iterator() );
        }
        catch ( Exception e )
        {
            throw new ProcedureException( ProcedureCallFailed, e, "Setting/removing the quarantine marker failed" );
        }
    }

    private <T extends Value> T extractParameter( AnyValue[] input, int index, String parameterName, Class<T> clazz )
    {
        if ( input.length <= index )
        {
            throw new IllegalArgumentException( "Illegal input:" + Arrays.toString( input ) );
        }
        var rawName = input[index];
        if ( !clazz.isInstance( rawName ) )
        {
            throw new IllegalArgumentException( format( "Parameter '%s' value should be a %s. Instead: %s", parameterName, clazz.getSimpleName(), rawName ) );
        }
        return clazz.cast( rawName );
    }
}
