#!/bin/bash

INPUT=pubsub://projects/practice-00001/topics/securecyber

/home/anaconda2/bin/python ../dataflow.py \
--input $INPUT \
--output gs://practice-00001/output/testoutput.txt \
--runner DataflowRunner \
--project practice-00001 \
--temp_location gs://practice-00001/temp