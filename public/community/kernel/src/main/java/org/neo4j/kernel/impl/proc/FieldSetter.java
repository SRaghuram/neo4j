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
package org.neo4j.kernel.impl.proc;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Field;

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * On calling apply, injects the `value` for the field `field` on the provided `object`.
 */
public class FieldSetter
{
    private final Field field;
    private final MethodHandle setter;
    private final ComponentRegistry.Provider<?> provider;

    FieldSetter( Field field, MethodHandle setter, ComponentRegistry.Provider<?> provider )
    {
        this.field = field;
        this.setter = setter;
        this.provider = provider;
    }

    public void apply( org.neo4j.kernel.api.proc.Context ctx, Object object ) throws ProcedureException
    {
        try
        {
            setter.invoke( object, provider.apply( ctx ) );
        }
        catch ( Throwable e )
        {
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed, e,
                    "Unable to inject component to field `%s`, please ensure it is public and non-final: %s",
                    field.getName(), e.getMessage() );
        }
    }

    public Object get( org.neo4j.kernel.api.proc.Context ctx ) throws ProcedureException
    {
        try
        {
            return provider.apply( ctx );
        }
        catch ( Throwable e )
        {
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed, e,
                    "Unable to inject component to field `%s`, please ensure it is public and non-final: %s",
                    field.getName(), e.getMessage() );
        }
    }

    Field field()
    {
        return field;
    }
}
