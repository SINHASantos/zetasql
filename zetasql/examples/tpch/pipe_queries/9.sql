#
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Removes unnecessary subquery and internal SELECT and aliasing of
# n_name column by using EXTEND to compute expressions.
FROM
  part,
  supplier,
  lineitem,
  partsupp,
  orders,
  nation
|> WHERE
    s_suppkey = l_suppkey
    AND ps_suppkey = l_suppkey
    AND ps_partkey = l_partkey
    AND p_partkey = l_partkey
    AND o_orderkey = l_orderkey
    AND s_nationkey = n_nationkey
    AND p_name LIKE '%tomato%'
|> EXTEND
    EXTRACT(year FROM o_orderdate) AS o_year,
    l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
|> AGGREGATE
    sum(amount) AS sum_profit
   GROUP BY
    n_name AS nation ASC,
    o_year DESC;
