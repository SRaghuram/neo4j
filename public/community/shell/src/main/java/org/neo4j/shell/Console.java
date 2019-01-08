/*
 * Copyright (c) 2002-2019 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.shell;

/**
 * Mimics the class with the same name which exists in Java 1.6
 * (not in Java 1.5).
 */
public interface Console
{
    /**
     * Prints a formatted string to the console (System.out).
     * @param format the string/format to print.
     * @param args values used in conjunction with {@code format}.
     */
    void format( String format, Object... args );

    /**
     * @param prompt the prompt to display.
     * @return the next line read from the console (user input).
     */
    String readLine( String prompt );
}
