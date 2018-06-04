@ECHO OFF
rem Copyright (c) 2002-2018 "Neo4j,"
rem Neo4j Sweden AB [http://neo4j.com]
rem This file is a commercial add-on to Neo4j Enterprise Edition.

SETLOCAL

Powershell -NoProfile -NonInteractive -NoLogo -ExecutionPolicy Bypass -Command "try { Unblock-File -Path '%~dp0Neo4j-Management\*.*' -ErrorAction 'SilentlyContinue' } catch {};Import-Module '%~dp0Neo4j-Management.psd1'; Exit (Invoke-Neo4j %*)"
EXIT /B %ERRORLEVEL%
