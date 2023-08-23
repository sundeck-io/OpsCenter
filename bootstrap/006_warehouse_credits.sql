
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

CREATE OR REPLACE FUNCTION INTERNAL.WAREHOUSE_MULTIPLIER(warehouse_size varchar)
RETURNS NUMBER
AS
$$
-- duplicates the above view, data must be kept in sync
case
    when warehouse_size = 'X-Small' then 1
    when warehouse_size = 'Small' then 2
    when warehouse_size = 'Medium' then 4
    when warehouse_size = 'Large' then 8
    when warehouse_size = 'X-Large' then 16
    when warehouse_size = '2X-Large' then 32
    when warehouse_size = '3X-Large' then 64
    when warehouse_size = '4X-Large' then 128
    when warehouse_size = '5X-Large' then 256
    when warehouse_size = '6X-Large' then 512
    else null
end
$$;

CREATE OR REPLACE FUNCTION INTERNAL.WAREHOUSE_CREDITS_PER_MILLI(warehouse_size varchar)
RETURNS NUMBER
AS
$$
    -- credits per hour / seconds in an hour / milliseconds in a second
    zeroifnull(internal.warehouse_multiplier(warehouse_size))/3600/1000
$$;
