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
package org.neo4j.kernel.api.exceptions.schema;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.kernel.api.exceptions.Status;

public class DropConstraintFailureException extends SchemaKernelException
{
    private final SchemaDescriptorSupplier constraint;

    public DropConstraintFailureException( SchemaDescriptorSupplier constraint, TokenNameLookup lookup, Throwable cause )
    {
        super( Status.Schema.ConstraintDropFailed, cause, "Unable to drop constraint %s: %s", constraint.userDescription( lookup ), cause.getMessage() );
        this.constraint = constraint;
    }

    public DropConstraintFailureException( String nameOrSchema, Throwable cause )
    {
        // nameOrSchema is just 'name' or 'on schema'
        super( Status.Schema.ConstraintDropFailed, cause, "Unable to drop constraint `%s`: %s", nameOrSchema, cause.getMessage() );
        this.constraint = null;
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        if ( constraint == null )
        {
            return getMessage();
        }
        else
        {
            String message = "Unable to drop " + constraint.userDescription( tokenNameLookup );
            if ( getCause() instanceof KernelException )
            {
                KernelException cause = (KernelException) getCause();

                return String.format( "%s:%n%s", message, cause.getUserMessage( tokenNameLookup ) );
            }
            return message;
        }
    }
}
