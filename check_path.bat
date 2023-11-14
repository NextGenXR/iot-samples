@echo off
SET RelativePath=\_build\target-deps\python\Scripts
SET FullPath=%CD%%RelativePath%
SET "PathFound=0"

REM Split the PATH variable and compare each part with the desired path
FOR %%i IN ("%PATH:;=" "%") DO (
    IF "%%~i" == "%FullPath%" (
        SET PathFound=1
        GOTO PathCheckDone
    )
)

:PathCheckDone
IF "%PathFound%"=="0" (
    echo Adding %FullPath% to your PATH...
    setx PATH "%PATH%;%FullPath%"
) ELSE (
    echo %FullPath% is already in PATH.
)

REM Refreshing environment variables for this session
REM set PATH=%PATH%;%FullPath%
