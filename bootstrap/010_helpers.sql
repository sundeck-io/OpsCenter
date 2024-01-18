
CREATE OR REPLACE FUNCTION internal.friendly_duration(start_time TIMESTAMP_NTZ, end_time TIMESTAMP_NTZ)
RETURNS VARCHAR(50)
LANGUAGE SQL
AS
$$
  CASE
    WHEN DATEDIFF('second', start_time, end_time) < 180 THEN '<3 minutes'
    WHEN DATEDIFF('second', start_time, end_time) >= 180 AND DATEDIFF('second', start_time, end_time) < 600 THEN '3-10 minutes'
    WHEN DATEDIFF('second', start_time, end_time) >= 600 AND DATEDIFF('second', start_time, end_time) < 3600 THEN '10 minutes - 1 hour'
    WHEN DATEDIFF('second', start_time, end_time) >= 3600 AND DATEDIFF('second', start_time, end_time) < 14400 THEN '1 to 4 hours'
    WHEN DATEDIFF('second', start_time, end_time) >= 14400 AND DATEDIFF('second', start_time, end_time) < 86400 THEN '4-24 hours'
    WHEN DATEDIFF('second', start_time, end_time) >= 86400 AND DATEDIFF('second', start_time, end_time) < 604800 THEN '1-7 days'
    WHEN DATEDIFF('second', start_time, end_time) >= 604800 AND DATEDIFF('second', start_time, end_time) < 2592000 THEN '7-30 days'
    ELSE '>30 days'
  END
$$;

CREATE OR REPLACE FUNCTION internal.friendly_duration(duration_millis NUMBER)
RETURNS STRING
LANGUAGE SQL
AS
$$
  CASE
    WHEN duration_millis < 180000 THEN '<3 minutes'
    WHEN duration_millis >= 180000 AND duration_millis < 600000 THEN '3-10 minutes'
    WHEN duration_millis >= 600000 AND duration_millis < 3600000 THEN '10 minutes - 1 hour'
    WHEN duration_millis >= 3600000 AND duration_millis < 14400000 THEN '1 to 4 hours'
    WHEN duration_millis >= 14400000 AND duration_millis < 86400000 THEN '4-24 hours'
    WHEN duration_millis >= 86400000 AND duration_millis < 604800000 THEN '1-7 days'
    WHEN duration_millis >= 604800000 AND duration_millis < 2592000000 THEN '7-30 days'
    ELSE '>30 days'
  END
$$;

CREATE OR REPLACE FUNCTION internal.friendly_duration_ordinal(duration_millis NUMBER)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
  CASE
    WHEN duration_millis < 180000 THEN 0
    WHEN duration_millis >= 180000 AND duration_millis < 600000 THEN 1
    WHEN duration_millis >= 600000 AND duration_millis < 3600000 THEN 2
    WHEN duration_millis >= 3600000 AND duration_millis < 14400000 THEN 3
    WHEN duration_millis >= 14400000 AND duration_millis < 86400000 THEN 4
    WHEN duration_millis >= 86400000 AND duration_millis < 604800000 THEN 5
    WHEN duration_millis >= 604800000 AND duration_millis < 2592000000 THEN 6
    ELSE 7
  END
$$;



CREATE OR REPLACE FUNCTION internal.emonth(val TIMESTAMP)
RETURNS INTEGER
LANGUAGE SQL
AS
$$
   DATEDIFF('month', '1970-01-01', val)::INTEGER
$$;


CREATE OR REPLACE FUNCTION internal.period_range(period string, st timestamp, ed timestamp)
RETURNS ARRAY
LANGUAGE SQL
AS
$$
  case
      WHEN period = 'second' then split(repeat(',', DATEDIFF(second, st, ed)), ',')
      WHEN period = 'SECOND' then split(repeat(',', DATEDIFF(second, st, ed)), ',')
      WHEN period = 'minute' then split(repeat(',', DATEDIFF(minute, st, ed)), ',')
      WHEN period = 'MINUTE' then split(repeat(',', DATEDIFF(minute, st, ed)), ',')
      WHEN period = 'day' then split(repeat(',', DATEDIFF(day, st, ed)), ',')
      WHEN period = 'DAY' then split(repeat(',', DATEDIFF(day, st, ed)), ',')
      WHEN period = 'hour' then split(repeat(',', DATEDIFF(hour, st, ed)), ',')
      WHEN period = 'HOUR' then split(repeat(',', DATEDIFF(hour, st, ed)), ',')
  end
$$
;

CREATE OR REPLACE FUNCTION internal.period_range_plus(period string, st timestamp, ed timestamp)
RETURNS ARRAY
LANGUAGE SQL
AS
$$
  case
      WHEN period = 'second' then split(repeat(',', DATEDIFF(second, st, ed) + 1), ',')
      WHEN period = 'SECOND' then split(repeat(',', DATEDIFF(second, st, ed) + 1), ',')
      WHEN period = 'minute' then split(repeat(',', DATEDIFF(minute, st, ed) + 1), ',')
      WHEN period = 'MINUTE' then split(repeat(',', DATEDIFF(minute, st, ed) + 1), ',')
      WHEN period = 'day' then split(repeat(',', DATEDIFF(day, st, ed) + 1), ',')
      WHEN period = 'DAY' then split(repeat(',', DATEDIFF(day, st, ed) + 1), ',')
      WHEN period = 'hour' then split(repeat(',', DATEDIFF(hour, st, ed) + 1), ',')
      WHEN period = 'HOUR' then split(repeat(',', DATEDIFF(hour, st, ed) + 1), ',')
  end
$$
;

-- Nice that they added array_generate_range recently!!
CREATE OR REPLACE FUNCTION internal.rangesql(st INTEGER, end INTEGER)
RETURNS ARRAY
LANGUAGE SQL
AS
$$
    ARRAY_GENERATE_RANGE(st, end)
$$;

CREATE FUNCTION IF NOT EXISTS tools.qtag(text varchar, all1 boolean, all2 boolean)
    returns array
  AS
   $$array_construct()$$
  ;

CREATE OR REPLACE VIEW internal.dates AS
SELECT DATEADD(DAY, -1 * ROW_NUMBER() OVER (ORDER BY seq4()), CURRENT_DATE())::timestamp AS date
FROM TABLE(GENERATOR(ROWCOUNT => 2 * 365));

create or replace function tools.template(template string, json_data string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'run'
AS
$$
import json
def run(template, json_data):
    data = json.loads(json_data)
    return template.format(**data)
$$;

create or replace function tools.templatejs(template string, data variant)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  const regex = /{([^{}]+)}/g;
  const matches = TEMPLATE.match(regex);
  str = TEMPLATE;
  if (matches) {
    for (const match of matches) {
      const variable = match.slice(1, -1);
      if (!DATA.hasOwnProperty(variable)) {
        throw 'Value not found for variable: ' + variable;
      }
      str = str.replace(match, DATA[variable]);
    }
  }

  return str;
$$;
