
create view if not exists reporting.sundeck_query_history as select * from data.sundeck_query_history;
grant select on reporting.sundeck_query_history to application role admin;
