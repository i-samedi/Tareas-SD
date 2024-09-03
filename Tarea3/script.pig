branch_traces = LOAD '/user/hive/warehouse/branch_traces/branch_traces.csv' USING PigStorage(',') AS (branch_addr:chararray, branch_type:chararray, taken:chararray, target:chararray);

DUMP branch_traces;

--paso 1
record_count = FOREACH (GROUP branch_traces ALL) GENERATE COUNT_STAR(branch_traces);
DUMP record_count;

branch_traces = LOAD '/user/hive/warehouse/branch_traces/branch_traces.csv' USING PigStorage(',') AS (branch_addr:chararray, branch_type:chararray, taken:chararray, target:chararray);

--paso 2: 

grouped_by_branch = GROUP branch_traces BY branch_type;
branch_frequency = FOREACH grouped_by_branch GENERATE group AS branch_type, COUNT(branch_traces) AS frequency;
DUMP branch_frequency;

--paso 3:


grouped_by_branch_taken = GROUP branch_traces BY (branch_type, taken);
branch_taken_frequency = FOREACH grouped_by_branch_taken GENERATE group.branch_type AS branch_type, group.taken AS taken, COUNT(branch_traces) AS frequency;
DUMP branch_taken_frequency;

--paso 4:

--se convierte taken a int
branch_traces_with_int = FOREACH branch_traces GENERATE branch_addr, branch_type, (int)taken AS taken_int, target;
branch_taken_1 = FILTER branch_traces_with_int BY taken_int == 1;
grouped_by_branch_all = GROUP branch_traces_with_int BY branch_type;
grouped_by_branch_taken_1 = GROUP branch_taken_1 BY branch_type;

total_per_branch = FOREACH grouped_by_branch_all GENERATE group AS branch_type, COUNT(branch_traces_with_int) AS total_count;

taken_per_branch = FOREACH grouped_by_branch_taken_1 GENERATE group AS branch_type, COUNT(branch_taken_1) AS taken_count;

branch_counts = JOIN total_per_branch BY branch_type, taken_per_branch BY branch_type;

branch_proportion = FOREACH branch_counts GENERATE total_per_branch::branch_type AS branch_type, (double)taken_per_branch::taken_count / total_per_branch::total_count AS taken_proportion;

DUMP branch_proportion;

--EXPLICADO:

-- Cargar datos asegurándonos de que 'taken' sea tratado como 'chararray'
branch_traces = LOAD '/user/hive/warehouse/branch_traces/branch_traces.csv' USING PigStorage(',') AS (branch_addr:chararray, branch_type:chararray, taken:chararray, target:chararray);

-- Convertir 'taken' a 'int'
branch_traces_with_int = FOREACH branch_traces GENERATE branch_addr, branch_type, (int)taken AS taken_int, target;

-- Filtrar los registros con 'taken' igual a 1
branch_taken_1 = FILTER branch_traces_with_int BY taken_int == 1;

-- Agrupar por 'branch_type'
grouped_by_branch_all = GROUP branch_traces_with_int BY branch_type;
grouped_by_branch_taken_1 = GROUP branch_taken_1 BY branch_type;

-- Calcular el total de registros para cada 'branch_type'
total_per_branch = FOREACH grouped_by_branch_all GENERATE group AS branch_type, COUNT(branch_traces_with_int) AS total_count;

-- Calcular el total de registros con 'taken' igual a 1 para cada 'branch_type'
taken_per_branch = FOREACH grouped_by_branch_taken_1 GENERATE group AS branch_type, COUNT(branch_taken_1) AS taken_count;

-- Juntar ambos resultados y calcular la proporción
branch_counts = JOIN total_per_branch BY branch_type, taken_per_branch BY branch_type;
branch_proportion = FOREACH branch_counts GENERATE total_per_branch::branch_type AS branch_type, (double)taken_per_branch::taken_count / total_per_branch::total_count AS taken_proportion;

-- Mostrar el resultado
DUMP branch_proportion;

--paso 7
sampled_branch_traces = SAMPLE branch_traces 0.1;

--se aumenta el porcentaje:
sampled_branch_traces = SAMPLE branch_traces 0.10;