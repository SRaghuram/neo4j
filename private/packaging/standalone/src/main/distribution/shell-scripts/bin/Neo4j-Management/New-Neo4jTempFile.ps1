#
# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.
#


<#
.SYNOPSIS
Returns a temporary filename with optional prefix

.DESCRIPTION
Returns a temporary filename with optional prefix

.PARAMETER Prefix
Optional prefix to for the temporary filename

.EXAMPLE
New-Neo4jTempFile

Returns a temporary filename

.OUTPUTS
String Filename of a temporary file which does not yet exist

.NOTES
This function is private to the powershell module

#>
Function New-Neo4jTempFile
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low')]
  param (
    [Parameter(Mandatory=$false,ValueFromPipeline=$false)]
    [String]$Prefix = ''
  )

  Begin {
  }
  
  Process {
    Do { 
      $RandomFileName = Join-Path -Path ([System.IO.Path]::GetTempPath()) -ChildPath ($Prefix + [System.IO.Path]::GetRandomFileName())
    } 
    Until (-not (Test-Path -Path $RandomFileName))

    return $RandomFileName
  }
  
  End {
  }
}
