import sthelp
import session
import config
import reports_query_activity
import reports_top_spenders
import reports_dbt
import reports

sthelp.chrome("Reports")
sess = session.reports().get_report()
credit_cost = config.get_compute_credit_cost()

options = {
    "Query Activity": reports_query_activity.report,
    "Top Spenders": reports_top_spenders.report,
    "dbt Summary": reports_dbt.report,
}

reports.display(options)
