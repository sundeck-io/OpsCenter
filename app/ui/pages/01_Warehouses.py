import sthelp
import reports_warehouse
import reports_heatmap
import reports

sthelp.chrome("Reports")

options = {
    "Warehouse Activity": reports_warehouse.report,
    "Warehouse Heatmap": reports_heatmap.heatmap,
}

reports.display(options)
