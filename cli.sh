#!/bin/bash
if [ $1 ==  "dev" ];
then
 export dbenv="dev";
 echo "Using Dev Database"
else
 export dbenv="prod";
 echo "Using Prod Database"
fi