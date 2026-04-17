create or replace dynamic table IRRI_PREPROD.SILVER.TARIF_CESSION_ESPAGNE_RAPIDE(
	TYPE_COMMANDE,
	CODE_ARTICLE,
	TARIF_HT_CESS_ESPAGNE,
	CONDITIONNEMENT,
	QUANTITE,
	CONDITIONNEMENT_BASE,
	TARIF_AU_CONDITIONNEMENT,
	DATE_UPDATE
) target_lag = '1 minute' refresh_mode = FULL initialize = ON_CREATE warehouse = COMPUTE_WH
 as
WITH
    tarif_a_l_unite AS (
        SELECT
            tarif.code_article AS code_article,
            tarif.tarif_ht_cess_espagne AS tarif_ht_cess_espagne,
            article.cleaned_attribute_value AS Conditionnement,
            1 AS Quantite,
            CASE
            WHEN  REPLACE(article.attribute_data, '"', '') = 'M' THEN 'CM'
            WHEN  REPLACE(article.attribute_data, '"', '') = 'L' THEN 'ML'
            ELSE REPLACE(article.attribute_data, '"', '') END AS Conditionnement_Base,
            tarif_ht_cess_espagne AS Tarif_au_conditionnement,
            tarif.DATE_UPD_CESS_ESPAGNE AS DATE_UPDATE
        FROM
            SILVER.TARIF_CESSION_ESPAGNE tarif
            INNER JOIN SILVER.ARTICLE_ATTRIBUTES_VALUE_ES article
                ON tarif.code_article = article.sku
                AND article.attribute_code = 'UNITE_VENTE_FINALE'
    ),
    tarif_conditionnement_non_obligatoire AS (
        SELECT
            tarif.code_article AS code_article,
            tarif.tarif_ht_cess_espagne AS tarif_ht_cess_espagne,
            CASE article.attribute_code
                WHEN 'QUANTITE_CARTON' THEN 'CAR' || try_to_number(article.cleaned_attribute_value)
                WHEN 'QUANTITE_CARTON_MAGASIN' THEN 'CAR' || try_to_number(article.cleaned_attribute_value)
                WHEN 'quantite_par_palette' THEN 'PAL' || try_to_number(article.cleaned_attribute_value)
                ELSE article.attribute_code
            END AS Conditionnement,
            try_to_number(article.cleaned_attribute_value) AS Quantite,
             CASE
            WHEN  REPLACE(article.attribute_data, '"', '') = 'M' THEN 'CM'
            WHEN  REPLACE(article.attribute_data, '"', '') = 'L' THEN 'ML'
            ELSE REPLACE(article.attribute_data, '"', '') END AS Conditionnement_Base,
            try_to_number(article.cleaned_attribute_value) * tarif_ht_cess_espagne AS Tarif_au_conditionnement,
            tarif.DATE_UPD_CESS_ESPAGNE AS DATE_UPDATE
        FROM
            SILVER.TARIF_CESSION_ESPAGNE tarif
            INNER JOIN SILVER.ARTICLE_ATTRIBUTES_VALUE_ES article
                ON tarif.code_article = article.sku
                AND try_to_number(cleaned_attribute_value) > 1.0
                AND article.attribute_code IN (
                    'quantite_par_palette',
                    'QUANTITE_CARTON',
                    'QUANTITE_CARTON_MAGASIN'
                )
            INNER JOIN SILVER.ARTICLE_ATTRIBUTES_VALUE_ES articleBase
                ON tarif.code_article = articleBase.sku
                AND articleBase.attribute_code = 'UNITE_VENTE_FINALE'
    ),
    ArticleRapideEspagne AS (
        SELECT ap.SKU AS Code_Article
        FROM SILVER.ARTICLE_ATTRIBUTES_VALUE_ES ap
        INNER JOIN SILVER.ARTICLE_ATTRIBUTES_VALUE_ES attr
            ON ap.sku = attr.sku
            AND attr.attribute_code = 'PAYS_COMMANDE_RAPIDE'
            AND attr.cleaned_attribute_value like '%ESPAGNE_METRO%'
        WHERE ap.ATTRIBUTE_CODE = 'CANAUX_VENTE_POSSIBLES'
        AND ap.ATTRIBUTE_DATA LIKE '%ESPAGNE%'
    )
SELECT
    DISTINCT 'Rapide' AS Type_Commande,
    CODE_ARTICLE,
    TARIF_HT_CESS_ESPAGNE,
    CONDITIONNEMENT,
    QUANTITE,
    CONDITIONNEMENT_BASE,
    TARIF_AU_CONDITIONNEMENT,
    DATE_UPDATE
FROM
    (
        SELECT
            tarif.CODE_ARTICLE,
            tarif.TARIF_HT_CESS_ESPAGNE,
            tarif.CONDITIONNEMENT,
            tarif.QUANTITE,
            tarif.CONDITIONNEMENT_BASE,
            tarif.TARIF_AU_CONDITIONNEMENT,
            tarif.DATE_UPDATE
        FROM
            tarif_a_l_unite tarif
            INNER JOIN ArticleRapideEspagne article ON tarif.code_article = article.code_article
        UNION
        SELECT
            tarif.CODE_ARTICLE,
            tarif.TARIF_HT_CESS_ESPAGNE,
            tarif.CONDITIONNEMENT,
            tarif.QUANTITE,
            tarif.CONDITIONNEMENT_BASE,
            tarif.TARIF_AU_CONDITIONNEMENT,
            tarif.DATE_UPDATE
        FROM
            tarif_conditionnement_non_obligatoire tarif
            INNER JOIN ArticleRapideEspagne article ON tarif.code_article = article.code_article
    ) a
;

ALTER DYNAMIC TABLE IRRI_PREPROD.SILVER.TARIF_CESSION_ESPAGNE_RAPIDE REFRESH;

SELECT * FROM IRRI_PREPROD.SILVER.TARIF_CESSION_ESPAGNE_REAPPRO where code_article = '434141';

