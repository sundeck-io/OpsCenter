
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

CREATE OR REPLACE FUNCTION INTERNAL.WAREHOUSE_MULTIPLIER(warehouse_size varchar, warehouse_type varchar)
RETURNS NUMBER
AS
$$
-- duplicates the above view, data must be kept in sync
case
    when warehouse_size = 'X-Small' and warehouse_type = 'STANDARD' then 1
    when warehouse_size = 'Small' and warehouse_type = 'STANDARD' then 2
    when warehouse_size = 'Medium' and warehouse_type = 'STANDARD' then 4
    when warehouse_size = 'Large' and warehouse_type = 'STANDARD' then 8
    when warehouse_size = 'X-Large' and warehouse_type = 'STANDARD' then 16
    when warehouse_size = '2X-Large' and warehouse_type = 'STANDARD' then 32
    when warehouse_size = '3X-Large' and warehouse_type = 'STANDARD' then 64
    when warehouse_size = '4X-Large' and warehouse_type = 'STANDARD' then 128
    when warehouse_size = '5X-Large' and warehouse_type = 'STANDARD' then 256
    when warehouse_size = '6X-Large' and warehouse_type = 'STANDARD' then 512
    when warehouse_size = 'Medium' and warehouse_type = 'SNOWPARK-OPTIMIZED' then 4*1.5
    when warehouse_size = 'Large' and warehouse_type = 'SNOWPARK-OPTIMIZED' then 8*1.5
    when warehouse_size = 'X-Large' and warehouse_type = 'SNOWPARK-OPTIMIZED' then 16*1.5
    when warehouse_size = '2X-Large' and warehouse_type = 'SNOWPARK-OPTIMIZED' then 32*1.5
    when warehouse_size = '3X-Large' and warehouse_type = 'SNOWPARK-OPTIMIZED' then 64*1.5
    when warehouse_size = '4X-Large' and warehouse_type = 'SNOWPARK-OPTIMIZED' then 128*1.5
    when warehouse_size = '5X-Large' and warehouse_type = 'SNOWPARK-OPTIMIZED' then 256*1.5
    when warehouse_size = '6X-Large' and warehouse_type = 'SNOWPARK-OPTIMIZED' then 512*1.5
    else null
end
$$;

CREATE OR REPLACE FUNCTION INTERNAL.WAREHOUSE_CREDITS_PER_MILLI(warehouse_size varchar, warehouse_type varchar)
RETURNS NUMBER
AS
$$
    -- credits per hour / seconds in an hour / milliseconds in a second
    zeroifnull(internal.warehouse_multiplier(warehouse_size, warehouse_type))/3600/1000
$$;

CREATE OR REPLACE FUNCTION TOOLS.WAREHOUSE_CREDITS_PER_MILLI(warehouse_size varchar, warehouse_type varchar)
RETURNS NUMBER
AS
$$
    internal.warehouse_credits_per_milli(warehouse_size, warehouse_type)
$$;

CREATE OR REPLACE FUNCTION TOOLS.WAREHOUSE_MULTIPLIER(warehouse_size varchar, warehouse_type varchar)
RETURNS NUMBER
AS
$$
    internal.warehouse_multiplier(warehouse_size, warehouse_type)
$$;

CREATE OR REPLACE FUNCTION INTERNAL.WAREHOUSE_CREDITS_PER_MILLI(warehouse_size varchar)
RETURNS NUMBER
AS
$$
    -- credits per hour / seconds in an hour / milliseconds in a second
    zeroifnull(internal.warehouse_multiplier(warehouse_size, 'STANDARD'))/3600/1000
$$;

CREATE OR REPLACE FUNCTION TOOLS.WAREHOUSE_CREDITS_PER_MILLI(warehouse_size varchar)
RETURNS NUMBER
AS
$$
    internal.warehouse_credits_per_milli(warehouse_size)
$$;

CREATE OR REPLACE FUNCTION TOOLS.WAREHOUSE_MULTIPLIER(warehouse_size varchar)
RETURNS NUMBER
AS
$$
    internal.warehouse_multiplier(warehouse_size)
$$;
