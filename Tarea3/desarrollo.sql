/* parte1 */
SELECT COUNT(*) FROM branch_traces;

/* parte2 */
SELECT branch_type, COUNT(*) AS frequency FROM branch_traces GROUP BY branch_type;

/* parte3 */
SELECT branch_type, taken, COUNT(*) AS frequency FROM branch_traces GROUP BY branch_type, taken;

/* parte4 */
SELECT branch_type, SUM(CASE WHEN taken = 1 THEN 1 ELSE 0 END) / COUNT(*) AS taken_proportion FROM branch_traces GROUP BY branch_type;

/* parte5 */
/* Tabla para frecuencia de cada tipo de branch */
CREATE TABLE branch_type_frequency AS SELECT branch_type, COUNT(*) AS frequency FROM branch_traces GROUP BY branch_type;

/* Tabla para la relación entre tipos de branch y "taken" */
CREATE TABLE branch_taken_relationship AS SELECT branch_type, taken, COUNT(*) AS frequency FROM branch_traces GROUP BY branch_type, taken;

/* Tabla para la proporción de "taken" igual a 1 */
CREATE TABLE branch_taken_proportion AS SELECT branch_type, SUM(CASE WHEN taken = 1 THEN 1 ELSE 0 END) / COUNT(*) AS taken_proportion FROM branch_traces GROUP BY branch_type;

/* PARTE7 */

CREATE TABLE branch_traces_sampled AS SELECT * FROM branch_traces TABLESAMPLE (10 PERCENT);
SELECT COUNT(*) FROM branch_traces_sampled;

/* Se cambian los porcentajes*/
CREATE TABLE branch_traces_sampled AS SELECT * FROM branch_traces TABLESAMPLE (80 PERCENT);