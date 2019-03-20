:: 
:: Copyright 2013 Intel Corporation, All Rights Reserved.
:: 
:: Licensed under the Apache License, Version 2.0 (the "License");
:: you may not use this file except in compliance with the License.
:: You may obtain a copy of the License at
:: 
::    http://www.apache.org/licenses/LICENSE-2.0
:: 
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.
:: 

@echo off

if "%1_" == "_" (
	echo This script helps to generate one deliverable package.
	echo 
	echo Usage:
	echo         %0 {version}
	exit -1
)

echo "Build up main structure"
SET ARTIFACT_DIR=artifact/%1
mkdir artifact
mkdir %ARTIFACT_DIR%

xcopy /Y /E release\* %ARTIFACT_DIR%\
xcopy /Y /E dist\* %ARTIFACT_DIR%\

xcopy /Y LICENSE %ARTIFACT_DIR%
xcopy /Y NOTICE %ARTIFACT_DIR%
xcopy /Y VERSION %ARTIFACT_DIR%

xcopy /Y README.md %ARTIFACT_DIR%
xcopy /Y BUILD.md %ARTIFACT_DIR%

xcopy /Y CHANGELOG %ARTIFACT_DIR%
xcopy /Y TODO.md %ARTIFACT_DIR%

xcopy /Y COSBenchUserGuide.pdf %ARTIFACT_DIR%
xcopy /Y COSBenchAdaptorDevGuide.pdf %ARTIFACT_DIR%
xcopy /Y 3rd-party-licenses.pdf %ARTIFACT_DIR%
xcopy /Y pkg.lst %ARTIFACT_DIR%

echo "Build up adaptor example enviornment"
xcopy /Y /E ext\* %ARTIFACT_DIR%\ext\

