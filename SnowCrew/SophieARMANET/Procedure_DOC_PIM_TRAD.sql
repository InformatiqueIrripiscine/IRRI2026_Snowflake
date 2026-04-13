// 13-04-2026

CREATE OR REPLACE PROCEDURE AREA_SOPHIE.PUBLIC.ANALYSE_ET_COPIE_DOCUMENTS()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
import zipfile
import os
import re
from snowflake.snowpark.files import SnowflakeFile

STAGE = '@AREA_SOPHIE.PUBLIC.DOCUMENTS_PIM_TRAD'

def extract_pdfs_from_zips(session):
    already_done = session.sql(
        "SELECT ZIP_NAME FROM AREA_SOPHIE.PUBLIC.ZIPS_TRAITES"
    ).collect()
    done_set = {row['ZIP_NAME'] for row in already_done}

    all_files = session.sql(
        f"SELECT RELATIVE_PATH FROM DIRECTORY('{STAGE}') WHERE RELATIVE_PATH ILIKE '%.zip'"
    ).collect()
    new_zips = [row['RELATIVE_PATH'] for row in all_files if row['RELATIVE_PATH'] not in done_set]

    if not new_zips:
        return [], 0

    extracted_pdfs = []
    for zip_path in new_zips:
        full_path = f'{STAGE}/{zip_path}'
        nb_pdfs = 0
        try:
            with SnowflakeFile.open(full_path, 'rb', require_scoped_url=False) as f:
                with zipfile.ZipFile(f, 'r') as zf:
                    pdf_entries = [name for name in zf.namelist() if name.lower().endswith('.pdf')]
                    for pdf_name in pdf_entries:
                        safe_name = re.sub(r'[^\w.\-]', '_', os.path.basename(pdf_name))
                        code_match = re.match(r'^(\d{6})', safe_name)
                        if not code_match:
                            parts = pdf_name.split('/')
                            for part in parts:
                               ain m = re.match(r'^(\d{6})', part)
                                if m:
                                    safe_name = m.group(1) + '_' + safe_name
                                    break
                        tmp_path = f'/tmp/{safe_name}'
                        with open(tmp_path, 'wb') as out:
                            out.write(zf.read(pdf_name))
                        session.file.put(
                            tmp_path, STAGE,
                            auto_compress=False, overwrite=True
                        )
                        extracted_pdfs.append(safe_name)
                        nb_pdfs += 1
        except Exception as e:
            pass

        session.sql(
            f"INSERT INTO AREA_SOPHIE.PUBLIC.ZIPS_TRAITES (ZIP_NAME, NB_PDFS_EXTRAITS) "
            f"VALUES ('{zip_path}', {nb_pdfs})"
        ).collect()

    session.sql(f"ALTER STAGE AREA_SOPHIE.PUBLIC.DOCUMENTS_PIM_TRAD REFRESH").collect()
    return extracted_pdfs, len(new_zips)


def main(session):
    from datetime import date
    today_folder = date.today().strftime('%Y%m%d')

    extracted_pdfs, nb_new_zips = extract_pdfs_from_zips(session)

    session.sql(f"""
        CREATE OR REPLACE TABLE AREA_SOPHIE.PUBLIC.DOCUMENTS_LANGUES AS
        WITH stage_files AS (
            SELECT RELATIVE_PATH AS file_name
            FROM DIRECTORY('{STAGE}')
            WHERE RELATIVE_PATH ILIKE '%.pdf'
              AND NOT REGEXP_LIKE(RELATIVE_PATH, '^[0-9]{{8}}/.*')
              AND NOT RELATIVE_PATH ILIKE '%.zip'
        ),
        doc_contents AS (
            SELECT
                file_name AS document_name,
                CASE
                    WHEN REGEXP_LIKE(file_name, '^[0-9]{{6}}_.*')
                    THEN LEFT(file_name, 6)
                    ELSE NULL
                END AS code_article,
                SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                    '{STAGE}',
                    file_name,
                    {{'mode': 'OCR'}}
                ):content::VARCHAR AS content
            FROM stage_files
        ),
        lang_analysis AS (
            SELECT
                document_name,
                code_article,
                content,
                REGEXP_COUNT(content, '[А-Яа-яЁё]') > 50 AS russian,
                SNOWFLAKE.CORTEX.COMPLETE(
                    'llama3.1-70b',
                    'Analyze this text and return ONLY a valid JSON: {{"english":true,"french":false,"spanish":false,"italian":false,"german":false,"portuguese":false,"turkish":false,"czech":false,"dutch":false,"polish":false,"chinese":false,"japanese":false,"arabic":false}}. Set true for languages present in the text. Text sample: ' || LEFT(content, 30000) || ' ... [end sample, also check middle]: ' || SUBSTR(content, GREATEST(1, LENGTH(content)/2 - 5000), 10000)
                ) AS lang_json
            FROM doc_contents
        ),
        parsed AS (
            SELECT
                document_name,
                code_article,
                russian,
                TRY_PARSE_JSON(REGEXP_SUBSTR(lang_json, '\\\\{{[^}}]+\\\\}}')) AS js
            FROM lang_analysis
        )
        SELECT
            document_name,
            code_article,
            COALESCE(js:english::BOOLEAN, FALSE) AS english,
            COALESCE(js:french::BOOLEAN, FALSE) AS french,
            COALESCE(js:spanish::BOOLEAN, FALSE) AS spanish,
            COALESCE(js:italian::BOOLEAN, FALSE) AS italian,
            COALESCE(js:german::BOOLEAN, FALSE) AS german,
            COALESCE(js:portuguese::BOOLEAN, FALSE) AS portuguese,
            COALESCE(js:turkish::BOOLEAN, FALSE) AS turkish,
            COALESCE(js:czech::BOOLEAN, FALSE) AS czech,
            COALESCE(js:dutch::BOOLEAN, FALSE) AS dutch,
            COALESCE(js:polish::BOOLEAN, FALSE) AS polish,
            COALESCE(js:chinese::BOOLEAN, FALSE) AS chinese,
            COALESCE(js:japanese::BOOLEAN, FALSE) AS japanese,
            COALESCE(js:arabic::BOOLEAN, FALSE) AS arabic,
            russian
        FROM parsed
    """).collect()

    nb_docs_row = session.sql("SELECT COUNT(*) AS cnt FROM AREA_SOPHIE.PUBLIC.DOCUMENTS_LANGUES").collect()
    nb_docs = nb_docs_row[0]['CNT']

    session.sql(f"""
        CREATE OR REPLACE TABLE AREA_SOPHIE.PUBLIC.COPIES_A_FAIRE AS
        WITH lang_codes AS (
            SELECT 'english' AS lang_name, 'en_GB' AS lang_code UNION ALL
            SELECT 'french', 'fr_FR' UNION ALL
            SELECT 'spanish', 'es_ES' UNION ALL
            SELECT 'italian', 'it_IT' UNION ALL
            SELECT 'german', 'de_DE' UNION ALL
            SELECT 'portuguese', 'pt_PT' UNION ALL
            SELECT 'dutch', 'nl_BE'
        ),
        docs_with_article AS (
            SELECT * FROM AREA_SOPHIE.PUBLIC.DOCUMENTS_LANGUES
            WHERE code_article IS NOT NULL
        ),
        unpivoted AS (
            SELECT document_name, code_article, 'english' AS lang_name, english AS has_lang FROM docs_with_article UNION ALL
            SELECT document_name, code_article, 'french', french FROM docs_with_article UNION ALL
            SELECT document_name, code_article, 'spanish', spanish FROM docs_with_article UNION ALL
            SELECT document_name, code_article, 'italian', italian FROM docs_with_article UNION ALL
            SELECT document_name, code_article, 'german', german FROM docs_with_article UNION ALL
            SELECT document_name, code_article, 'portuguese', portuguese FROM docs_with_article UNION ALL
            SELECT document_name, code_article, 'dutch', dutch FROM docs_with_article
        )
        SELECT
            u.document_name AS source_file,
            '{today_folder}/'
                || REGEXP_SUBSTR(u.document_name, '(.*)[.][^.]+$', 1, 1, 'e')
                || '_' || lc.lang_code
                || '.'
                || REGEXP_SUBSTR(u.document_name, '[.]([^.]+)$', 1, 1, 'e')
            AS target_file,
            lc.lang_code,
            u.code_article
        FROM unpivoted u
        JOIN lang_codes lc ON u.lang_name = lc.lang_name
        WHERE u.has_lang = TRUE
    """).collect()

    nb_copies_row = session.sql("SELECT COUNT(*) AS cnt FROM AREA_SOPHIE.PUBLIC.COPIES_A_FAIRE").collect()
    nb_copies = nb_copies_row[0]['CNT']

    session.sql(f"""
        COPY FILES
            INTO {STAGE}
            FROM (
                SELECT
                    '{STAGE}/' || source_file,
                    target_file
                FROM AREA_SOPHIE.PUBLIC.COPIES_A_FAIRE
            )c'est 
    """).collect()

    return f'Analyse terminée: {nb_new_zips} nouveaux ZIPs, {nb_docs} documents analysés, {nb_copies} copies créées dans le dossier {today_folder}'
$$;

TRUNCATE TABLE AREA_SOPHIE.PUBLIC.ZIPS_TRAITES;

CALL AREA_SOPHIE.PUBLIC.ANALYSE_ET_COPIE_DOCUMENTS();