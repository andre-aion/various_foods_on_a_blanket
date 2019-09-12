from concurrent.futures import ThreadPoolExecutor

from bokeh.models import WidgetBox, Spacer
from tornado import gen
from bokeh.document import without_document_lock

# Bokeh basics
from bokeh.models.widgets import Tabs, CheckboxGroup, Button, Panel, Div
from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.layouts import gridplot

from tornado.ioloop import IOLoop

# GET THE DASHBOARD
from scripts.dashboards.picnic_test.beiber import twitter_loader_tab

# UTILS
from scripts.utils.mylogger import mylogger
logger = mylogger(__file__)
executor = ThreadPoolExecutor(max_workers=10)

labels = [
    'twitter search',
]
DEFAULT_CHECKBOX_SELECTION = 0


@gen.coroutine
def aion_analytics(doc):
    class SelectionTab:
        def __init__(self):
            self.selected_tabs = []
            self.tablist = []
            self.selected_tracker = []  # used to monitor if a tab has already been launched
            self.div_style = """ style='width:300px; margin-left:-200%;
                       border:1px solid #ddd;border-radius:3px;background:#efefef50;' 
                       """
            self.page_width = 1200

        def notification_updater(self, text):
            txt = """<div style="text-align:center;background:black;width:100%;">
                     <h4 style="color:#fff;">
                     {}</h4></div>""".format(text)
            return txt

        def get_selections(self, checkboxes):
            self.selected_tabs = [checkboxes.labels[i] for i in checkboxes.active]
            return self.selected_tabs

    selection_tab = SelectionTab()
    # SETUP BOKEH OBJECTS
    try:
        tablist = []
        TABS = Tabs(tabs=tablist)
        @gen.coroutine
        def load_callstack(tablist):
            lst = selection_tab.get_selections(selection_checkboxes)
            #logger.warning('selections:%s',lst)

            panel_title = 'twitter search'
            if panel_title in lst:
                if panel_title not in selection_tab.selected_tracker:
                    tw = yield twitter_loader_tab(panel_title=panel_title)
                    selection_tab.selected_tracker.append(panel_title)
                    if tw not in tablist:
                        tablist.append(tw)

            # make list unique
            tablist = list(set(tablist))
            TABS.update(tabs=tablist)

        @gen.coroutine
        def select_tabs():
            notification_div.text = """
                <div style="text-align:center;background:black;width:{}px;margin-bottom:100px;">
                        <h1 style="color:#fff;margin-bottom:300px;">{}</h1>
                </div>""".format(selection_tab.page_width,'Tabs are loading')

            yield load_callstack(tablist)

            notification_div.text = """
                <div style="text-align:center;background:black;width:{}px;margin-bottom:100px;">
                        <h1 style="color:#fff;margin-bottom:300px">{}</h1>
                </div>""".format(selection_tab.page_width,'Welcome to the Picnic Data Science Portal')

        @gen.coroutine
        def update_selected_tabs():
            notification_div.text = """
                <div style="text-align:center;background:black;width:{}px;margin-bottom:100px;">
                        <h1 style="color:#fff;margin-bottom:300px">{}</h1>
                </div>""".format(selection_tab.page_width)

            doc.clear()
            tablist = []
            selection_checkboxes.active=[]

            mgmt = Panel(child=grid, title='Tab Selection')
            tablist.append(mgmt)
            TABS.update(tabs=tablist)
            doc.add_root(TABS)
            yield load_callstack(tablist)
            notification_div.text = """
                <div style="text-align:center;background:black;width:{}px;margin-bottom:100px;">
                        <h1 style="color:#fff;margin-bottom:300px">{}</h1>
                </div>""".format(selection_tab.page_width,"Welcome to the Picnic Data Science Portal")


        # -----------------------
        txt = """
                <div {}>
                <h3 style='color:blue;text-align:center'>Info:</h3>
                <ul style='margin-top:-10px;height:200px;'>
                <li>
                Select the tab(s) you want activated
                </li>
                <li>
                Then click the 'launch activity' button.
                </li>
                </ul>
            </div>
            """.format(selection_tab.div_style)

        information_div = Div(text=txt, width=400, height=250)
        footer_div = Div(text="""<hr/><div style="width:{}px;height:{}px;
                              position:relative;background:black;"></div>"""
                         .format(selection_tab.page_width,50),
                         width=selection_tab.page_width, height=100)
        txt = """
            <div style="text-align:center;background:black;width:{}px;margin-bottom:100px;">
                    <h1 style="color:#fff;margin-bottom:300px">{}</h1>
            </div>""".format(selection_tab.page_width,'Welcome to the Picnic Data Science Portal')
        notification_div = Div(text=txt,width=selection_tab.page_width,height=40)

        # choose startup tabs
        selection_checkboxes = CheckboxGroup(labels=labels, active=[DEFAULT_CHECKBOX_SELECTION])
        run_button = Button(label='Launch tabs', button_type="success")
        run_button.on_click(select_tabs)

        # setup layout
        controls = WidgetBox(selection_checkboxes, run_button)

        # create the dashboards
        grid = gridplot([
            [notification_div],
            [Spacer(width=50, height=2, sizing_mode='scale_width')],
            [controls,information_div],
            [footer_div]
        ])

        # setup launch tabs
        mgmt = Panel(child=grid, title='Tab Selection')

        tablist.append(mgmt)
        TABS.update(tabs=tablist)
        doc.add_root(TABS)
    except Exception:
        logger.error("TABS:", exc_info=True)

# configure and run bokeh server
@gen.coroutine
@without_document_lock
def launch_server():
    try:
        apps = {"/analytics": Application(FunctionHandler(aion_analytics))}
        io_loop = IOLoop.current()
        server = Server(apps,port=5006, allow_websocket_origin=["*"],io_loop=io_loop,
                        session_ids='signed',relative_urls=False)
        server.start()
        server.io_loop.add_callback(server.show, '/analytics')
        server.io_loop.start()
    except Exception:
        logger.error("WEBSERVER LAUNCH:", exc_info=True)

if __name__ == '__main__':
    launch_server()