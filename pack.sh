#!/bin/bash
#
#Copyright 2013 Intel Corporation, All Rights Reserved.
#
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.
#

if [ $# -lt 1 ];
then
        echo Usage:  $0 {version}
        exit -1
fi

ARTIFACT_DIR="artifact/$1"

echo "Build up main structure"

mkdir -p "$ARTIFACT_DIR"

cp -f -R release/* "$ARTIFACT_DIR"/
cp -f -R dist/* "$ARTIFACT_DIR"/

cp -f LICENSE "$ARTIFACT_DIR"/
cp -f NOTICE "$ARTIFACT_DIR"/
cp -f VERSION "$ARTIFACT_DIR"/

cp -f README.md "$ARTIFACT_DIR"/
cp -f BUILD.md "$ARTIFACT_DIR"/

cp -f CHANGELOG "$ARTIFACT_DIR"/
cp -f TODO.md "$ARTIFACT_DIR"/

cp -f COSBenchUserGuide.pdf "$ARTIFACT_DIR"/
cp -f COSBenchAdaptorDevGuide.pdf "$ARTIFACT_DIR"/
cp -f 3rd-party-licenses.pdf "$ARTIFACT_DIR"/
cp -f pkg.lst "$ARTIFACT_DIR"/


echo "Build up adaptor example enviornment"
mkdir "$ARTIFACT_DIR"/ext
cp -f -R ext/* "$ARTIFACT_DIR"/ext
