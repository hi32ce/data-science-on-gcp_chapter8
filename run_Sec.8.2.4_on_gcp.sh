#!/usr/bin/env bash
mvn -Pdataflow-runner compile exec:java -Dexec.mainClass=com.google.cloud.datascienceongcp.flights.CreateTrainingDataset_8_2_3 -Dexec.args="--project=ds-gcp-252814 --stagingLocation=gs://dsongcp_20190914/staging/ --input=gs://dsongcp_20190914/flights/chapter8/small.csv --output=gs://dsongcp_20190914/flights/chapter8/output/ --runner=DataflowRunner"
