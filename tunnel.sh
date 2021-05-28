#!/bin/sh
ssh -f -i ~/.ssh/id_rsa benno_grottenegg@3.215.176.179 -L 3306:production-us.ca2tik3n0kik.us-east-1.rds.amazonaws.com:3306 -N
