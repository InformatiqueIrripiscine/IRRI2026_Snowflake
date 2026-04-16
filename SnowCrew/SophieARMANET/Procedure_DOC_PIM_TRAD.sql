CREATE OR REPLACE PROCEDURE AREA_SOPHIE.PUBLIC.ANALYSE_ET_COPIE_DOCUMENTS()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
import zipfile
import os
import re
from snowflake.snowpark.files import SnowflakeFile

STAGE = ''@AREA_SOPHIE.PUBLIC.DOCUMENTS_PIM_TRAD''

# Extraction du nom des zips + stockage dans done_set si déjà fait
def extract_pdfs_from_zips(session):
    already_done = session.sql(
        "SELECT ZIP_NAME FROM AREA_SOPHIE.PUBLIC.ZIPS_TRAITES"
    ).collect()
    done_set = {row[''ZIP_NAME''] for row in already_done}

# Select from a directory de tous les noms des zips
    all_files = session.sql(
        f"SELECT RELATIVE_PATH FROM DIRECTORY(''{STAGE}'') WHERE RELATIVE_PATH ILIKE ''%.zip''"
    ).collect()
    new_zips = [row[''RELATIVE_PATH''] for row in all_files if row[''RELATIVE_PATH''] not in done_set]

    if not new_zips:
        return ''Aucun nouveau ZIP à traiter''

    extracted_pdfs = []
    file_type_map = {}
    for zip_path in new_zips:
        full_path = f''{STAGE}/{zip_path}''
        nb_pdfs = 0
        type_zip = None
        # Ouvre le dossier zip en mode fichier python, un objet SnowflakeFile que python peut lire
        try:
            with SnowflakeFile.open(full_path, ''rb'', require_scoped_url=False) as f:
# Ouvre le fichier zip et lit l\'index puis list les noms de toutes les paths via namelist() en filtrant par tous ce qui finit par pdf, png ou jpg et retourne un tableau
                with zipfile.ZipFile(f, ''r'') as zf:
                    pdf_entries = [name for name in zf.namelist() if name.lower().endswith((''.pdf'', ''.png'', ''.jpg''))]

# Split de tous le chemin par / et retourne un tableau, utilise l index pour détecter le sku, récupérer le type de dcuments et break dès qu un est trouvé

                    for pdf_name in pdf_entries:
                        parts = pdf_name.split(''/'')
                        entry_type = None
                        for i, part in enumerate(parts):
                            if re.match(r''^\\d+$'', part) and i + 1 < len(parts) - 1:
                                entry_type = parts[i + 1]
                                break
                        if entry_type is None and len(parts) > 2:
                            candidate = parts[-2]
                            if not re.match(r''^\\d+$'', candidate):
                                entry_type = candidate
                        if type_zip is None and entry_type:
                            type_zip = entry_type

# Replace les espaces et accents par des _ enfin d être safe sur le nom, recupere tous les lettre, chiffre ,_ , - et . dans la regex et le nom du pdf seul via .basename()

                        safe_name = re.sub(r''[^\\w.\\-]'', ''_'', os.path.basename(pdf_name))
                        code_match = re.match(r''^(\\d+)'', safe_name)
                        if not code_match:
                             for part in parts:
                                 m = re.match(r''^(\\d+)'', part)
                                 if m:
                                     safe_name = m.group(1) + ''_'' + safe_name
                                     break
                        tmp_path = f''/tmp/{safe_name}''

        # Ajout dans tmp_path de tous les pdf_name récupérés dans le zip, zf.read décompresse et lit le contenu binaire pour écrire le pdf avec ces octets (ces données)
                        with open(tmp_path, ''wb'') as out:
                            out.write(zf.read(pdf_name))
                            
        # Méthode Snowflake pour uploader des fichiers dans un stage
                        session.file.put(
                            tmp_path, STAGE,
                            auto_compress=False, overwrite=True
                        )
                        extracted_pdfs.append(safe_name)
                        if entry_type:
                            file_type_map[safe_name] = entry_type
                        nb_pdfs += 1
        except Exception as e:
            pass

        type_zip_sql = f"''{type_zip}''" if type_zip else "NULL"
        session.sql(
            f"INSERT INTO AREA_SOPHIE.PUBLIC.ZIPS_TRAITES (ZIP_NAME, TYPE_ZIP, NB_PDFS_EXTRAITS) "
            f"VALUES (''{zip_path}'', {type_zip_sql}, {nb_pdfs})"
        ).collect()

# Une fois tout insérer dans le stage on refresh le stage (collect execute une requete sql)   

    session.sql(f"ALTER STAGE AREA_SOPHIE.PUBLIC.DOCUMENTS_PIM_TRAD REFRESH").collect()
    return extracted_pdfs, len(new_zips), file_type_map


def main(session):
    from datetime import date
    today_folder = date.today().strftime(''%Y%m%d'')

    extracted_pdfs, nb_new_zips, file_type_map = extract_pdfs_from_zips(session)

# file_type_name ressemble à : "clé": "safe_name" ; "valeur" : "type_zip", la requête permet d unifier des select avec le file_name reprenant la clé et le file_typ la valur pour le mettre dans ue table temporaire
    if file_type_map:
        type_map_values = " UNION ALL ".join(
            [f"SELECT ''{k}'' AS file_name, ''{v}'' AS file_type" for k, v in file_type_map.items()]
        )
        session.sql(f"""
            CREATE OR REPLACE TEMPORARY TABLE AREA_SOPHIE.PUBLIC.TMP_FILE_TYPE_MAP AS
            {type_map_values}
        """).collect()
    else:
        session.sql("""
            CREATE OR REPLACE TEMPORARY TABLE AREA_SOPHIE.PUBLIC.TMP_FILE_TYPE_MAP (
                FILE_NAME VARCHAR, FILE_TYPE VARCHAR
            )
        """).collect()

    session.sql("""
        CREATE TABLE IF NOT EXISTS AREA_SOPHIE.PUBLIC.DOCUMENTS_LANGUES (
            DOCUMENT_NAME VARCHAR, CODE_ARTICLE VARCHAR,
            ENGLISH BOOLEAN, FRENCH BOOLEAN, SPANISH BOOLEAN,
            ITALIAN BOOLEAN, GERMAN BOOLEAN, PORTUGUESE BOOLEAN,
            TURKISH BOOLEAN, CZECH BOOLEAN, DUTCH BOOLEAN,
            POLISH BOOLEAN, CHINESE BOOLEAN, JAPANESE BOOLEAN,
            ARABIC BOOLEAN, RUSSIAN BOOLEAN
        )
    """).collect()

    new_files = session.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM DIRECTORY(''{STAGE}'')
        WHERE (RELATIVE_PATH ILIKE ''%.pdf'' OR RELATIVE_PATH ILIKE ''%.png'' OR RELATIVE_PATH ILIKE ''%.jpg'')
          AND NOT REGEXP_LIKE(RELATIVE_PATH, ''^[0-9]{{8}}/.*'')
          AND NOT RELATIVE_PATH ILIKE ''%.zip''
          AND RELATIVE_PATH NOT IN (
              SELECT DOCUMENT_NAME FROM AREA_SOPHIE.PUBLIC.DOCUMENTS_LANGUES
          )
    """).collect()

    if new_files[0][''CNT''] == 0:
        return ''Aucun nouveau fichier détecté''

    session.sql(f"""
        INSERT INTO AREA_SOPHIE.PUBLIC.DOCUMENTS_LANGUES
        WITH stage_files AS (
            SELECT RELATIVE_PATH AS file_name
            FROM DIRECTORY(''{STAGE}'')
            WHERE (RELATIVE_PATH ILIKE ''%.pdf'' OR RELATIVE_PATH ILIKE ''%.png'' OR RELATIVE_PATH ILIKE ''%.jpg'')
              AND NOT REGEXP_LIKE(RELATIVE_PATH, ''^[0-9]{{8}}/.*'')
              AND NOT RELATIVE_PATH ILIKE ''%.zip''
              AND RELATIVE_PATH NOT IN (
                  SELECT DOCUMENT_NAME
                  FROM AREA_SOPHIE.PUBLIC.DOCUMENTS_LANGUES
              )
        ),
        doc_contents AS (
            SELECT
                file_name AS document_name,
                REGEXP_SUBSTR(file_name, ''^(\\d+)'', 1, 1, ''e'') AS code_article,
                SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                    ''{STAGE}'',
                    file_name,
                    {{''mode'': ''OCR''}}
                ):content::VARCHAR AS content
            FROM stage_files
        ),
        lang_analysis AS (
            SELECT
                document_name,
                code_article,
                content,
                REGEXP_COUNT(content, ''[А-Яа-яЁё]'') > 50 AS russian,
                SNOWFLAKE.CORTEX.COMPLETE(
                    ''llama3.1-70b'',
                    ''Analyze this text and return ONLY a valid JSON: {{"english":true,"french":false,"spanish":false,"italian":false,"german":false,"portuguese":false,"turkish":false,"czech":false,"dutch":false,"polish":false,"chinese":false,"japanese":false,"arabic":false}}. Set true for languages present in the text. Text sample: '' || LEFT(content, 30000) || '' ... [end sample, also check middle]: '' || SUBSTR(content, GREATEST(1, LENGTH(content)/2 - 5000), 10000)
                ) AS lang_json
            FROM doc_contents
        ),
        parsed AS (
            SELECT
                document_name,
                code_article,
                russian,
                TRY_PARSE_JSON(REGEXP_SUBSTR(lang_json, ''\\\\\\\\{{[^}}]+\\\\\\\\}}'')) AS js
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
    nb_docs = nb_docs_row[0][''CNT'']

    session.sql("""
        CREATE TABLE IF NOT EXISTS AREA_SOPHIE.PUBLIC.COPIES_A_FAIRE (
            SOURCE_FILE VARCHAR, TARGET_FILE VARCHAR,
            LANG_CODE VARCHAR, CODE_ARTICLE VARCHAR,
            DATE_TRAITEMENT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """).collect()

    session.sql(f"""
        INSERT INTO AREA_SOPHIE.PUBLIC.COPIES_A_FAIRE
        WITH lang_codes AS (
            SELECT ''english'' AS lang_name, ''en_GB'' AS lang_code UNION ALL
            SELECT ''french'', ''fr_FR'' UNION ALL
            SELECT ''spanish'', ''es_ES'' UNION ALL
            SELECT ''italian'', ''it_IT'' UNION ALL
            SELECT ''german'', ''de_DE'' UNION ALL
            SELECT ''portuguese'', ''pt_PT'' UNION ALL
            SELECT ''dutch'', ''nl_BE''
        ),
        docs_to_copy AS (
            SELECT * FROM AREA_SOPHIE.PUBLIC.DOCUMENTS_LANGUES
            WHERE document_name NOT IN (
                  SELECT DISTINCT source_file
                  FROM AREA_SOPHIE.PUBLIC.COPIES_A_FAIRE
              )
        ),
        unpivoted AS (
            SELECT document_name, code_article, ''english'' AS lang_name, english AS has_lang FROM docs_to_copy UNION ALL
            SELECT document_name, code_article, ''french'', french FROM docs_to_copy UNION ALL
            SELECT document_name, code_article, ''spanish'', spanish FROM docs_to_copy UNION ALL
            SELECT document_name, code_article, ''italian'', italian FROM docs_to_copy UNION ALL
            SELECT document_name, code_article, ''german'', german FROM docs_to_copy UNION ALL
            SELECT document_name, code_article, ''portuguese'', portuguese FROM docs_to_copy UNION ALL
            SELECT document_name, code_article, ''dutch'', dutch FROM docs_to_copy
        )
        SELECT
            u.document_name AS source_file,
            ''{today_folder}/'' || COALESCE(ftm.file_type, ''autres'') || ''/''
                || REGEXP_SUBSTR(u.document_name, ''^([^_\\-]+)'', 1, 1, ''e'')
                || SUBSTR(u.document_name, LENGTH(REGEXP_SUBSTR(u.document_name, ''^([^_\\-]+)'', 1, 1, ''e'')) + 1, 1)
                || lc.lang_code
                || SUBSTR(u.document_name, LENGTH(REGEXP_SUBSTR(u.document_name, ''^([^_\\-]+)'', 1, 1, ''e'')) + 1)
            AS target_file,
            lc.lang_code,
            u.code_article
        FROM unpivoted u
        JOIN lang_codes lc ON u.lang_name = lc.lang_name
        LEFT JOIN AREA_SOPHIE.PUBLIC.TMP_FILE_TYPE_MAP ftm ON u.document_name = ftm.file_name
        WHERE u.has_lang = TRUE
    """).collect()

    nb_copies_row = session.sql("SELECT COUNT(*) AS cnt FROM AREA_SOPHIE.PUBLIC.COPIES_A_FAIRE").collect()
    nb_copies = nb_copies_row[0][''CNT'']

    copies_to_do = session.sql(f"""
        SELECT source_file, target_file
        FROM AREA_SOPHIE.PUBLIC.COPIES_A_FAIRE
        WHERE target_file LIKE ''{today_folder}/%''
    """).collect()

    for row in copies_to_do:
        src = row[''SOURCE_FILE'']
        tgt = row[''TARGET_FILE'']
        tgt_dir = tgt.rsplit(''/'', 1)[0]
        tgt_name = tgt.rsplit(''/'', 1)[1]
        try:
            session.sql(f"""
                COPY FILES INTO {STAGE}/{tgt_dir}/
                FROM (SELECT ''{STAGE}/{src}'', ''{tgt_name}'')
            """).collect()
        except Exception as e:
            pass

    non_zip_files = session.sql(f"""
        SELECT RELATIVE_PATH
        FROM DIRECTORY(''{STAGE}'')
        WHERE RELATIVE_PATH NOT LIKE ''%{today_folder}''
    """).collect()

    for row in non_zip_files:
        try:
            session.sql(f"REMOVE ''{STAGE}/{row[''RELATIVE_PATH'']}''").collect()
        except Exception as e:
            pass

    session.sql(f"ALTER STAGE AREA_SOPHIE.PUBLIC.DOCUMENTS_PIM_TRAD REFRESH").collect()

    session.sql(f"TRUNCATE TABLE AREA_SOPHIE.PUBLIC.ZIPS_TRAITES;").collect()


    return f''Analyse terminée: {nb_new_zips} nouveaux ZIPs, {nb_docs} documents analysés, {nb_copies} copies créées dans le dossier {today_folder}''
';


// POUR UNE REQUËTE PLUS RAPIDE ET UNE MEILLEURE PUISSANCE, ON PASSE PONCTUELLEMENT LA WAREHOUSE EN LARGE
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'LARGE';
CALL AREA_SOPHIE.PUBLIC.ANALYSE_ET_COPIE_DOCUMENTS();
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'XSMALL';

TRUNCATE TABLE AREA_SOPHIE.PUBLIC.COPIES_A_FAIRE;
TRUNCATE TABLE AREA_SOPHIE.PUBLIC.ZIPS_TRAITES;
TRUNCATE TABLE AREA_SOPHIE.PUBLIC.DOCUMENTS_LANGUES;

REMOVE '@AREA_SOPHIE.PUBLIC.DOCUMENTS_PIM_TRAD/150080_Capture_d_écran_2025-12-09_095854.png';
REMOVE @AREA_SOPHIE.PUBLIC.DOCUMENTS_PIM_TRAD PATTERN='.*\\.png';
REMOVE @AREA_SOPHIE.PUBLIC.DOCUMENTS_PIM_TRAD PATTERN='.*\\.jpg';


SELECT RELATIVE_PATH
        FROM DIRECTORY('@AREA_SOPHIE.PUBLIC.DOCUMENTS_PIM_TRAD')
        WHERE RELATIVE_PATH ILIKE '%440824%'

