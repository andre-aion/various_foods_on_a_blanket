
# when a tab does not work
from bokeh.models import Div, Panel
# handle weird twitter times
from datetime import datetime, timezone
import pytz


def tab_error_flag(tabname):
    # Make a tab with the layout
    text = """ERROR CREATING {} TAB, 
    CHECK THE LOGS""".format(tabname.upper())
    div = Div(text=text,
              width=200, height=100)

    tab = Panel(child=div, title=tabname)

    return tab