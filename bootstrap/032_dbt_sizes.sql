
-- udf to translate dbt model runtime into a broad size category
create or replace function tools.model_run_time(total_elapsed_time number)
returns varchar
as
$$
case
when total_elapsed_time < 60*1000 then 'XS'
when total_elapsed_time < 300*1000 then 'S'
when total_elapsed_time < 900*1000 then 'M'
when total_elapsed_time < 1800*1000 then 'L'
when total_elapsed_time < 3600*1000 then 'XL'
else 'XL+'
end
$$;

-- udf to translate bytes written by a dbt model to a broad size category
create or replace function tools.model_size_bytes(bytes_written_to_result number)
returns number
as
$$
case
when bytes_written_to_result < 1000000000 then 0 -- 1GB
when bytes_written_to_result < 10*1000000000 then 1  -- 10GB
when bytes_written_to_result < 100*1000000000 then 2 -- 100GB
when bytes_written_to_result < 1000*1000000000 then 3 -- 1TB
else 4
end
$$;

-- udf to translate rows produced into a broad size category
create or replace function tools.model_size_rows(rows_produced number)
returns number
as
$$
case
when rows_produced < 1000000 then 0
when rows_produced < 10000000 then 1
when rows_produced < 100000000 then 2
when rows_produced < 1000000000 then 3
else 4
end
$$;

-- combine rows and bytes produced category according to gitlab's model
create or replace function tools.model_size(rows_produced number, bytes_written_to_result number)
returns varchar
as
$$
case
when greatest(rows_produced, bytes_written_to_result) < 1 then 'XS'
when greatest(rows_produced, bytes_written_to_result) < 2 then 'S'
when greatest(rows_produced, bytes_written_to_result) < 3 then 'M'
when greatest(rows_produced, bytes_written_to_result) < 4 then 'L'
else 'XL'
end
$$;

-- translate bytes spilled to a broad category according to gitlab's model
create or replace function tools.model_efficiency(bytes_spilled_local number, bytes_spilled_remote number)
returns varchar
as
$$
case
when bytes_spilled_local = 0 and bytes_spilled_remote = 0 then 'Good'
when bytes_spilled_local <= 5 * 1000000000 and bytes_spilled_remote = 0 then 'Acceptable' -- 5GB
when bytes_spilled_local >  5 * 1000000000 and bytes_spilled_remote = 0  then 'Poor'
else 'VeryPoor'
end
$$;
