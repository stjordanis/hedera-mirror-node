-- Loads data from CSV files to PostgreSQL

SELECT cleanup();
SELECT drop_constraints_and_indexes();

\echo ------------------------------
\echo COPY data to t_record_files from %%TMP_DIR%%/t_record_files
\echo ------------------------------
\copy t_record_files FROM '%%TMP_DIR%%/t_record_files' WITH CSV HEADER;


\echo ------------------------------
\echo COPY data to t_entities from %%TMP_DIR%%/t_entities
\echo ------------------------------
\copy t_entities FROM '%%TMP_DIR%%/t_entities' WITH CSV HEADER;


\echo ------------------------------
\echo COPY data to transaction from %%TMP_DIR%%/transaction
\echo ------------------------------
\copy transaction FROM '%%TMP_DIR%%/transaction' WITH CSV HEADER;


\echo ------------------------------
\echo COPY data to crypto_transfer from %%TMP_DIR%%/crypto_transfer
\echo ------------------------------
\copy crypto_transfer FROM '%%TMP_DIR%%/crypto_transfer' WITH CSV HEADER;


\echo ------------------------------
\echo COPY data to t_file_data from %%TMP_DIR%%/t_file_data
\echo ------------------------------
\copy t_file_data FROM '%%TMP_DIR%%/t_file_data' WITH CSV HEADER;

\echo ------------------------------
\echo COPY data to topic_message from %%TMP_DIR%%/topic_message
\echo ------------------------------
\copy topic_message FROM '%%TMP_DIR%%/topic_message' WITH CSV HEADER;

\echo ------------------------------
\echo COPY data to account_balances from %%TMP_DIR%%/account_balances
\echo ------------------------------
\copy account_balances FROM '%%TMP_DIR%%/account_balances' WITH CSV HEADER;

SELECT create_constraints_and_indexes();
