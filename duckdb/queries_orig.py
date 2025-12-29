import datetime
import subprocess
import json
import os
import time
import re
# import sys
# sys.path.append('../common')
# from result_mapping import result_mapping
'''
注意修改文件目录
'''

duckdb_path = 'duckdb-orig/duckdb/build/release/duckdb'
db_file = 'duckdb-orig/duckdb/import_data/ldbc_db.duckdb'

def format_value_duckdb(value, param_type):
    param_type = param_type.upper()
    if param_type in ["DATETIME", "TIMESTAMPTZ"]:
        value_sql = value.replace("T", " ")
        return f"TIMESTAMPTZ '{value_sql}'"
    elif param_type == "DATE":
        return f"DATE '{value}'"
    elif param_type in ["INT", "INT32", "INT64", "BIGINT", "ID"]:
        return value
    elif param_type == "STRING[]":
        items = value.split(";")
        quoted_items = [f"'{item}'" for item in items]
        return "[" + ", ".join(quoted_items) + "]"
    else:
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
 
def run_script(filename,perf_file):
    
    perf = subprocess.Popen(["python3","/d1/machine_performance_indicators/monitor_system_perf.py", perf_file]) 
    start = time.time()
    for i in range(10):
        result = subprocess.run([duckdb_path, db_file, '-json', '-f', filename],
                            capture_output=True, text=True)
    end = time.time()
    perf.kill()
    duration = (end - start)/10
    print(filename)
    print(f"-> {duration:.4f} seconds")
    return result.stdout.strip(), duration

def run_query(query_num, query_variant, query_spec, query_parameters,test ,perf_file):
    # 生成 SET variable 语句
    set_stmts = []
    for k, v in query_parameters.items():
        param_name, param_type = k.split(":")
        val = format_value_duckdb(v, param_type)
        set_stmts.append(f"SET variable {param_name} = {val};")

    full_sql ="SET GLOBAL TimeZone = 'Etc/UTC';\n"+ '\n'.join(set_stmts) + '\n' + query_spec + ';'
    
    perf = subprocess.Popen(["python3","/d1/machine_performance_indicators/monitor_system_perf.py", perf_file]) 
    start = time.time()
    for i in range(10):
        result = subprocess.run([duckdb_path, db_file, '-json', '-c', full_sql],
                            capture_output=True, text=True)
    end = time.time()
    perf.kill()

    # 直接返回 stdout
    
    return result.stdout.strip(), (end - start)/10

def run_precomputations(query_variants, batch_id, batch_type, sf, timings_file):
    if "4" in query_variants:
        perf_file_path = f'system_performance_orig'
        os.makedirs(perf_file_path, exist_ok=True)
        perf_file = f'{perf_file_path}/bi-4-precomputations.csv'
        result , duration=run_script("dml/precomp/bi-4.sql",perf_file)
        timings_file.write(f"DuckDB|{sf}|{batch_id}|{batch_type}|q4precomputation||{duration}\n")
    if "6" in query_variants:
        perf_file_path = f'system_performance_orig'
        os.makedirs(perf_file_path, exist_ok=True)
        perf_file = f'{perf_file_path}/bi-6-precomputations.csv'
        result,duration=run_script("dml/precomp/bi-6.sql",perf_file)
        timings_file.write(f"DuckDB|{sf}|{batch_id}|{batch_type}|q6precomputation||{duration}\n")
    if "19a" in query_variants or "19b" in query_variants:
        perf_file_path = f'system_performance_orig'
        os.makedirs(perf_file_path, exist_ok=True)
        perf_file = f'{perf_file_path}/bi-19-precomputations.csv'
        result,duration=run_script("dml/precomp/bi-19.sql",perf_file)
        timings_file.write(f"DuckDB|{sf}|{batch_id}|{batch_type}|q19precomputation||{duration}\n")
    if "20a" in query_variants or "20b" in query_variants:
        perf_file_path = f'system_performance_orig'
        os.makedirs(perf_file_path, exist_ok=True)
        perf_file = f'{perf_file_path}/bi-20-precomputations.csv'
        result,duration=run_script("dml/precomp/bi-20.sql",perf_file)
        timings_file.write(f"DuckDB|{sf}|{batch_id}|{batch_type}|q20precomputation||{duration}\n")

def run_queries(query_variants, parameter_csvs, sf, test, pgtuning,batch_id, batch_type, timings_file, results_file):
    start_total = time.time()

    for query_variant in query_variants:
        query_num = int(re.sub("[^0-9]", "", query_variant))
        query_subvariant = re.sub("[^ab]", "", query_variant)
        print(f"========================= Q {query_num:02d}{query_subvariant} =========================")

        query_file_path = f'queries/bi-{query_num}.sql'
        with open(query_file_path, 'r') as f:
            query_spec = f.read()

        parameters_csv = parameter_csvs[query_variant]

        i = 0
        for query_parameters in parameters_csv:
            i += 1
            perf_file_path = f'system_performance_orig/bi-{query_num}{query_subvariant}'
            os.makedirs(perf_file_path, exist_ok=True)
            perf_file = f'{perf_file_path}/parameters-{i}.csv'
            # DuckDB 执行
            results, duration = run_query(query_num, query_variant, query_spec, query_parameters, test,perf_file)

            # 参数简化
            query_parameters_simple = {k.split(":")[0]: v for k, v in query_parameters.items()}
            query_parameters_json = json.dumps(query_parameters_simple)

            timings_file.write(f"DuckDB|{sf}|{batch_id}|{batch_type}|{query_variant}|{query_parameters_json}|{duration}\n")
            timings_file.flush()
            results_file.write(f"{query_num}|{query_variant}|{query_parameters_json}|{results}\n")
            results_file.flush()

            if (test) or (not pgtuning and i == 5) or (pgtuning and i == 100):
                break

    return time.time() - start_total

