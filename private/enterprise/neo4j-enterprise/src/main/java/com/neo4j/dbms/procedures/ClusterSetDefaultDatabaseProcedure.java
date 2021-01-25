/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures;

import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;

import java.util.Arrays;

import org.neo4j.collection.RawIterator;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.procedure.Mode;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseNotFound;
import static org.neo4j.kernel.api.exceptions.Status.Procedure.ProcedureCallFailed;

public class ClusterSetDefaultDatabaseProcedure extends CallableProcedure.BasicProcedure
{
    private static final String PROCEDURE_NAME = "setDefaultDatabase";
    private static final String[] PROCEDURE_NAMESPACE = {"dbms", "cluster"};
    private static final String PARAMETER_NAME = "databaseName";
    private final DatabaseIdRepository idRepository;
    private final TopologyService topologyService;

    public ClusterSetDefaultDatabaseProcedure( DatabaseIdRepository idRepository, TopologyService topologyService )
    {
        super( procedureSignature( new QualifiedName( PROCEDURE_NAMESPACE, PROCEDURE_NAME ) )
                .in( PARAMETER_NAME, Neo4jTypes.NTString )
                .out( "result", Neo4jTypes.NTString )
                .description( "Change the default database to the provided value. The database must exist and the old default database must be stopped." )
                .systemProcedure()
                .admin( true )
                .mode( Mode.WRITE )
                .build() );
        this.idRepository = idRepository;
        this.topologyService = topologyService;
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> apply( org.neo4j.kernel.api.procedure.Context ctx, AnyValue[] input, ResourceTracker resourceTracker )
            throws ProcedureException
    {
        if ( !ctx.procedureCallContext().isSystemDatabase() )
        {
            throw new ProcedureException( ProcedureCallFailed,
                    "This is a system-only procedure and it should be executed against the system database: dbms.cluster.setDefaultDatabase" );
        }

        var newDefaultDbName = extractDatabaseName( input );

        InternalTransaction transaction = ctx.internalTransaction();
        try ( KernelTransaction.Revertable ignore = transaction.overrideWith( EnterpriseSecurityContext.AUTH_DISABLED ) )
        {
            Node oldDefaultDatabase = transaction.findNode( Label.label( "Database" ), "default", true );
            Node newDefaultDatabase = transaction.findNode( Label.label( "Database" ), "name", newDefaultDbName.name() );
            if ( newDefaultDatabase == null )
            {
                throw new ProcedureException( ProcedureCallFailed,
                        "New default database %s does not exist.", newDefaultDbName.name() );
            }
            if ( oldDefaultDatabase != null )
            {
                if ( oldDefaultDatabase.getId() == newDefaultDatabase.getId() )
                {
                    return RawIterator.<AnyValue[],ProcedureException>of(
                            new AnyValue[]{
                                    Values.stringValue( String.format( "Default database already set to %s, no change required", newDefaultDbName.name() ) )} );
                }
                var oldDefaultDbName = new NormalizedDatabaseName( (String) oldDefaultDatabase.getProperty( "name" ) );
                assertOldDefaultStopped( oldDefaultDbName );
                oldDefaultDatabase.setProperty( "default", false );
            }
            newDefaultDatabase.setProperty( "default", true );
            return RawIterator.<AnyValue[],ProcedureException>of(
                    new AnyValue[]{Values.stringValue( String.format( "Default database set to %s", newDefaultDbName.name() ) )} );
        }
    }

    private void assertOldDefaultStopped( NormalizedDatabaseName oldDefault ) throws ProcedureException
    {
        var databaseId = idRepository.getByName( oldDefault )
                                     .orElseThrow( () -> new ProcedureException( DatabaseNotFound, format(
                                             "Unable to retrieve the status for database with name %s because no database with this name exists!",
                                             oldDefault ) ) );

        var coreStates = topologyService.allCoreStatesForDatabase( databaseId );
        var rrStates = topologyService.allReadReplicaStatesForDatabase( databaseId );

        var dbStopped = true;
        for ( DiscoveryDatabaseState state : coreStates.values() )
        {
            dbStopped &= state.operatorState().equals( STOPPED );
        }
        for ( DiscoveryDatabaseState state : rrStates.values() )
        {
            dbStopped &= state.operatorState().equals( STOPPED );
        }

        if ( !dbStopped )
        {
            throw new ProcedureException( ProcedureCallFailed, String.format( "The old default database %s is not fully stopped", oldDefault.name() ) );
        }
    }

    protected NormalizedDatabaseName extractDatabaseName( AnyValue[] input )
    {
        if ( input.length != 1 )
        {
            throw new IllegalArgumentException( "Illegal input:" + Arrays.toString( input ) );
        }
        var rawName = input[0];
        if ( !( rawName instanceof TextValue) )
        {
            throw new IllegalArgumentException( format( "Parameter '%s' value should have a string representation. Instead: %s", PARAMETER_NAME, rawName ) );
        }
        return new NormalizedDatabaseName( ((TextValue) rawName).stringValue() );
    }
}
