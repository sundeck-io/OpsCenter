
CREATE OR REPLACE VIEW INTERNAL_REPORTING.WAREHOUSE_CREDITS_PER_SIZE AS
SELECT
    WAREHOUSE_SIZE,
    CREDIT_PER_HOUR/3600.0/1000 AS CREDITS_PER_MILLI
FROM
    (   SELECT
            *
        FROM
            (VALUES
            ('X-Small', 1),
            ('Small', 2),
            ('Medium', 4),
            ('Large', 8),
            ('X-Large', 16),
            ('2X-Large', 32),
            ('3X-Large', 64),
            ('4X-Large', 128),
            ('5X-Large', 256),
            ('6X-Large', 512)) AS t(WAREHOUSE_SIZE, CREDIT_PER_HOUR) )
;
