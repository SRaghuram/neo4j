#
# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.
#


<#
.SYNOPSIS
Invokes an external command

.DESCRIPTION
Invokes an external command using CALL operator with stderr redirected to stdout both being
captured.

.PARAMETER Command
The executable that will be invoked

.PARAMETER CommandArgs
A list of arguments that will be added to the invocation

.EXAMPLE
Invoke-ExternalCommand -Command java.exe -Args @('-version')

Start java.exe with arguments `-version` passed 

.OUTPUTS
System.Collections.Hashtable
exitCode
capturedOutput

.NOTES
This function is private to the powershell module

#>
Function Invoke-ExternalCommand
{
  [cmdletBinding(SupportsShouldProcess=$false,ConfirmImpact='Low')]
  param (
    [Parameter(Mandatory=$true,ValueFromPipeline=$false,Position=0)]
    [string]$Command = '',

    [parameter(Mandatory=$false,ValueFromRemainingArguments=$true)]
    [Object[]]$CommandArgs = @()
   )
  
  Begin
  {
  }

  Process
  {
    # This is a hack to make Windows 7 happy with quoted commands. If command is quoated but
    # does not include a space, Windows 7 produces an error about command path not found. 
    # So we need to selectively apply double quotes when command includes spaces. 
    If ($Command -match " ")
    {
        $Command = '"{0}"' -f $Command
    }
  
    Write-Verbose "Invoking $Command with arguments $($CommandArgs -join " ")"
    $Output = & "$Env:ComSpec" /C "$Command $($CommandArgs -join " ") 2>&1"
    Write-Verbose "Command returned with exit code $LastExitCode"

    Write-Output @{'exitCode' = $LastExitCode; 'capturedOutput' = $Output}
  }
  
  End
  {
  }
}
