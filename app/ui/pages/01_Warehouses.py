import sthelp
import reports_warehouse
import reports_heatmap
import reports

sthelp.chrome("Reports")

options = {
    "Warehouse Heatmap": reports_heatmap.heatmap,
    "Warehouse Activity": reports_warehouse.report,
}

reports.display(options)
