#! /bin/bash
psql -h localhost -U postgres -d bigdata << SQL
DELETE FROM output;
COPY output FROM '/Users/yoshidacojp/dev/workspace4.2/nicoimpression/data/output/part-r-00000';
SQL