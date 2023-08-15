
CREATE OR REPLACE PROCEDURE INTERNAL.REPORT_PAGE_VIEW(page string)
RETURNS text
LANGUAGE SQL
AS
BEGIN
    SYSTEM$LOG_INFO(OBJECT_CONSTRUCT('action', 'page_view', 'page', page));
    return '';
END;

CREATE OR REPLACE PROCEDURE INTERNAL.REPORT_ACTION(domain string, verb string)
RETURNS text
LANGUAGE SQL
AS
BEGIN
    SYSTEM$LOG_INFO(OBJECT_CONSTRUCT('action', verb, 'domain', domain));
    return '';
END;
