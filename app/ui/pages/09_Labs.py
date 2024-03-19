import streamlit as st
import sthelp
import session
import config
import reports_query_hash
import reports

sthelp.chrome("Labs")
sess = session.reports().get_report()
credit_cost = config.get_compute_credit_cost()

st.markdown(
    """
            These reports are a work in progress. The data should be accurate, but the presentation is not final.
            When the presentation is final, these reports will be moved to the main reports page.
            Please let us know if you have any feedback or suggestions on these reports by joining our [Slack community](http://bit.ly/sundeck-slack-community).
            """
)

options = {
    "Reoccurring Queries": reports_query_hash.report,
}

reports.display(options)
