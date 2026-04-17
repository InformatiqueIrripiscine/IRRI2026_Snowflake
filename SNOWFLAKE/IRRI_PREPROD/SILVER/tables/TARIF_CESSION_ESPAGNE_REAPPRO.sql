create or replace dynamic table IRRI_PREPROD.SILVER.TARIF_CESSION_ESPAGNE_REAPPRO(
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
tarif_conditionnement_obligatoire AS (
SELECT 
    tarif.code_article
    , tarif.tarif_ht_cess_espagne
    , 'CAR' || try_to_number(article.cleaned_attribute_value) AS Conditionnement
    , try_to_number(article.cleaned_attribute_value) Quantite
    , REPLACE(articleBase.attribute_data, '"', '') AS Conditionnement_Base    
    , try_to_number(article.cleaned_attribute_value) * tarif_ht_cess_espagne Tarif_au_conditionnement
    ,tarif.date_upd_cess_espagne AS Date_Update
FROM SILVER.TARIF_CESSION_ESPAGNE tarif
INNER JOIN SILVER.ARTICLE_ATTRIBUTES_VALUE_ES article 
    ON tarif.code_article = article.sku
    --AND try_to_number(cleaned_attribute_value) > 1.0        
    AND article.attribute_code = 'QUANTITE_CARTON_MAGASIN'
INNER JOIN SILVER.ARTICLE_ATTRIBUTES_VALUE_ES articleBase 
    ON tarif.code_article = articleBase.sku
    AND articleBase.attribute_code = 'UNITE_VENTE_FINALE'),
tarif_a_l_unite AS (
SELECT 
    tarif.code_article
    , tarif.tarif_ht_cess_espagne
    , article.cleaned_attribute_value AS Conditionnement
    , 1 Quantite
    , CASE
            WHEN  REPLACE(article.attribute_data, '"', '') = 'M' THEN 'CM'
            WHEN  REPLACE(article.attribute_data, '"', '') = 'L' THEN 'ML'
            ELSE REPLACE(article.attribute_data, '"', '') END AS Conditionnement_Base      
    , tarif_ht_cess_espagne Tarif_au_conditionnement
    ,tarif.date_upd_cess_espagne AS Date_Update    
FROM SILVER.TARIF_CESSION_ESPAGNE tarif
INNER JOIN SILVER.ARTICLE_ATTRIBUTES_VALUE_ES article 
    ON tarif.code_article = article.sku
    AND article.attribute_code = 'UNITE_VENTE_FINALE'
WHERE tarif.code_article NOT IN (SELECT code_article FROM tarif_conditionnement_obligatoire)
    ),
tarif_conditionnement_non_obligatoire AS (
SELECT 
    tarif.code_article
    , tarif.tarif_ht_cess_espagne
    , case article.attribute_code
        when 'QUANTITE_CARTON' THEN  'CAR' || try_to_number(article.cleaned_attribute_value)
        when 'quantite_par_palette' THEN  'PAL' || try_to_number(article.cleaned_attribute_value)
        else article.attribute_code
        end AS Conditionnement
    , try_to_number(article.cleaned_attribute_value) Quantite
    ,  CASE
            WHEN  REPLACE(article.attribute_data, '"', '') = 'M' THEN 'CM'
            WHEN  REPLACE(article.attribute_data, '"', '') = 'L' THEN 'ML'
            ELSE REPLACE(article.attribute_data, '"', '') END AS Conditionnement_Base
    , try_to_number(article.cleaned_attribute_value) * tarif_ht_cess_espagne Tarif_au_conditionnement
    ,tarif.date_upd_cess_espagne AS Date_Update    
FROM SILVER.TARIF_CESSION_ESPAGNE tarif
INNER JOIN SILVER.ARTICLE_ATTRIBUTES_VALUE_ES article 
    ON tarif.code_article = article.sku
    AND try_to_number(cleaned_attribute_value) > 1.0    
    AND article.attribute_code in (
        'quantite_par_palette',
        'QUANTITE_CARTON'
        )
INNER JOIN SILVER.ARTICLE_ATTRIBUTES_VALUE_ES articleBase 
    ON tarif.code_article = articleBase.sku
    AND articleBase.attribute_code = 'UNITE_VENTE_FINALE'
WHERE tarif.code_article NOT IN (SELECT code_article FROM tarif_conditionnement_obligatoire)

    )
    SELECT 
        --ROW_NUMBER() OVER (PARTITION BY code_article ORDER BY quantite DESC) AS num_ligne,
        'Reappro' TYPE_COMMANDE,
    	CODE_ARTICLE,
    	TARIF_HT_CESS_ESPAGNE,
    	CONDITIONNEMENT,
    	QUANTITE,CONDITIONNEMENT_BASE,
    	TARIF_AU_CONDITIONNEMENT,
        DATE_UPDATE
    FROM 
        (SELECT * FROM tarif_conditionnement_obligatoire
        UNION
        SELECT * FROM tarif_a_l_unite
        UNION
        SELECT * FROM tarif_conditionnement_non_obligatoire
        ) a
    ORDER BY code_article, quantite DESC;


