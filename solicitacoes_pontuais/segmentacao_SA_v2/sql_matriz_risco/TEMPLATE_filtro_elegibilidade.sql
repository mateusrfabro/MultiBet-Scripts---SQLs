-- TEMPLATE: Como adicionar filtro de elegibilidade manualmente
-- Aplicar nas tags que falharem na correção automática

-- ANTES (exemplo):
WITH qualifying AS (
    SELECT
        u.c_ecr_id AS user_id,
        'multibet' AS label_id,
        'TAG_NAME' AS tag,
        -10 AS score
    FROM ecr_ec2.tbl_ecr u
    WHERE [condições da tag]
)
SELECT * FROM qualifying

-- DEPOIS (com filtro elegibilidade):
WITH qualifying AS (
    SELECT
        u.c_ecr_id AS user_id,
        'multibet' AS label_id,
        'TAG_NAME' AS tag,
        -10 AS score
    FROM ecr_ec2.tbl_ecr u
    JOIN ecr_ec2.tbl_ecr_category cat ON u.c_ecr_id = cat.c_ecr_id
    WHERE cat.c_category IN ('play_user', 'real_user')
      AND [condições da tag]
)
SELECT * FROM qualifying

-- OU se a tag já usa JOIN complexo:
WITH qualifying AS (
    SELECT
        u.c_ecr_id AS user_id,
        'multibet' AS label_id,
        'TAG_NAME' AS tag,
        -10 AS score
    FROM ecr_ec2.tbl_ecr u
    JOIN ecr_ec2.tbl_ecr_category cat ON u.c_ecr_id = cat.c_ecr_id
    JOIN [outras tabelas...]
    WHERE cat.c_category IN ('play_user', 'real_user')
      AND [condições da tag]
)
SELECT * FROM qualifying