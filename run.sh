#!/usr/bin/env bash

# Author: Salman Rahman


ant clean dist &&
hadoop jar dist/TimeAnalysis.jar TimeAnalysis  src/main/resources/olympictweets2016rio.test out
