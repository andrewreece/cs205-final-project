#!/bin/bash

sudo mkdir /mnt1/data

sudo aws s3 sync s3://cs205-final-project/tweets/gardenhose/ /mnt1/data/