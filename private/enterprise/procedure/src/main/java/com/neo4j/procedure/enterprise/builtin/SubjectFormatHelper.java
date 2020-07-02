/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import org.neo4j.common.Subject;

class SubjectFormatHelper
{
    static String formatSubject( Subject subject )
    {
        if ( subject == Subject.ANONYMOUS || subject == Subject.AUTH_DISABLED || subject == Subject.SYSTEM )
        {
            return "";
        }

        return subject.getUsername();
    }
}
