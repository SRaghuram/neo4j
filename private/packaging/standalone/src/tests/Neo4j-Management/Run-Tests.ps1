#
# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.
#


# Run tests in this directory and subdirectories.
# Usage:
#     powershell.exe \\path\to\Run-Tests.ps1
#     powershell.exe -NonInteractive -ExecutionPolicy ByPass -File \\path\to\Run-Tests.ps1

$here = Split-Path -Parent $MyInvocation.MyCommand.Definition
Invoke-Pester -OutputFile pester-nunit.xml -OutputFormat NUnitXml -EnableExit -Script $here
