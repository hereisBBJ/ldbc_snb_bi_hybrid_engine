#!/bin/bash

# 定义一个数组，包含所有表的名称
dynamictables=("Comment" "Forum" "Post" "Person" "Comment_hasTag_Tag" "Post_hasTag_Tag" "Forum_hasMember_Person" "Forum_hasTag_Tag" "Person_hasInterest_Tag" "Person_likes_Comment" "Person_likes_Post" "Person_studyAt_University" "Person_workAt_Company" "Person_knows_Person")

# 合并多个 CSV 文件为一个文件，但跳过每个表的表头
for table in "${dynamictables[@]}"; do
  for file in /d1/ouf_sf_data/PostgreSQL_out_sf10_bi/graphs/csv/bi/composite-merged-fk/initial_snapshot/dynamic/${table}/*.csv; do
    tail -n +2 "$file" >> /d1/ouf_sf_data/merge_postgre_SF10_data/merged_${table}.csv
  done
done

statictables=("Organisation" "Place" "Tag" "TagClass")

for table in "${statictables[@]}"; do
  for file in /d1/ouf_sf_data/PostgreSQL_out_sf10_bi/graphs/csv/bi/composite-merged-fk/initial_snapshot/static/${table}/*.csv; do
    tail -n +2 "$file" >> /d1/ouf_sf_data/merge_postgre_SF10_data/merged_${table}.csv
  done
done
