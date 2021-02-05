/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public final class ShowDatabasesHelpers
{

    private ShowDatabasesHelpers()
    {
    }

    public static List<ShowDatabasesResultRow> showDatabases( DatabaseManagementService dbms )
    {
        var systemDb = (GraphDatabaseFacade) dbms.database( SYSTEM_DATABASE_NAME );
        List<ShowDatabasesResultRow> rows;

        try ( var tx = systemDb.beginTransaction( KernelTransaction.Type.EXPLICIT, EnterpriseSecurityContext.AUTH_DISABLED ) )
        {
            var result = tx.execute( "SHOW DATABASES" );
            try ( var resultRows = result.stream() )
            {
                rows = resultRows.map( ShowDatabasesResultRow::fromResult ).collect( Collectors.toList() );
            }
            tx.commit();
        }
        return rows;
    }

    public static class ShowDatabasesResultRow
    {
        private final String name;
        private final String address;
        private final String role;
        private final String requestedStatus;
        private final String currentStatus;
        private final String error;
        private final boolean isDefault;

        ShowDatabasesResultRow( String name, String address, String role, String requestedStatus, String currentStatus, String error, boolean isDefault )
        {
            this.name = name;
            this.address = address;
            this.role = role;
            this.requestedStatus = requestedStatus;
            this.currentStatus = currentStatus;
            this.error = error;
            this.isDefault = isDefault;
        }

        static ShowDatabasesResultRow fromResult( Map<String,Object> result )
        {
            var columnNames = List.of( "name", "address", "role", "requestedStatus", "currentStatus", "error", "default" );

            List<String> missingKeys = columnNames.stream().map( col -> Pair.of( col, result.get( col ) ) )
                            .filter( p -> Objects.isNull( p.other() ) )
                            .map( Pair::first )
                            .collect( Collectors.toList() );

            if ( !missingKeys.isEmpty() )
            {
                String missingKeyString = String.join( ", ", missingKeys );
                throw new IllegalArgumentException( format( "Some required columns are missing from the result: %s", missingKeyString ) );
            }

            Map<String,String> resultStrings = result.entrySet().stream()
                    .collect( Collectors.toMap( Map.Entry::getKey, e -> String.valueOf( e.getValue() ) ) );

            var name = resultStrings.get( "name" );
            var address = resultStrings.get( "address" );
            var role = resultStrings.get( "role" );
            var requestedStatus = resultStrings.get( "requestedStatus" );
            var currentStatus = resultStrings.get( "currentStatus" );
            var error = resultStrings.get( "error" );
            boolean isDefault = Boolean.parseBoolean( resultStrings.get( "default" ) );

            return new ShowDatabasesResultRow( name, address, role, requestedStatus, currentStatus, error, isDefault );
        }

        public String name()
        {
            return name;
        }

        public String address()
        {
            return address;
        }

        public String role()
        {
            return role;
        }

        public String requestedStatus()
        {
            return requestedStatus;
        }

        public String currentStatus()
        {
            return currentStatus;
        }

        public String error()
        {
            return error;
        }

        public boolean isDefault()
        {
            return isDefault;
        }

        @Override
        public String toString()
        {
            return "ShowDatabasesResultRow{" + "name='" + name + '\'' + ", address='" + address + '\'' + ", role='" + role + '\'' + ", requestedStatus='" +
                    requestedStatus + '\'' + ", currentStatus='" + currentStatus + '\'' + ", error='" + error + '\'' + ", isDefault=" + isDefault + '}';
        }
    }
}
