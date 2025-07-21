"""
Julyan van der Westhuizen
20/07/25

This is a Terminal User Interface for interacting with the pipeline.
"""

from pipeline_log_manager import shared_logger
import json
from textual.app import App, ComposeResult
from textual.containers import Container, Center
from textual.binding import Binding
from textual.widgets import Tabs, Tab, Header, Static, Footer, Digits
from textual.widget import Widget
from textual.containers import Vertical

# class Overview(Vertical):
#     diagram_template = """
#         +-------------------+                                                                 
#         |Data Source:       |                                                                 
#         |TFL Disruptions API|                                                                 
#         +-------------------+                                                                 
#                 |                                                                                 
#                 ▼                                                                                 
#         ┏━━━━━━━┓━━━━━━┓     ┏━━━━━━━━━┓━━━━━━━━┓     ┏━━━━┓━━━━━━━━━┓     +----------------+
#         ┃EXTRACT┃      ┃     ┃TRANSFORM┃        ┃     ┃LOAD┃         ┃     |Database:       |
#         ┗━━━━━━━┛      ┃     ┗━━━━━━━━━┛        ┃     ┗━━━━┛         ┃     |                |
#         ┃Last Fetch:   ┃     ┃Data Transformed: ┃     ┃Last Load:    ┃     |Last added rows:|
#         ┃[]            ┃{arr}┃[]                ┃----►┃[]            ┃----►|[]              |
#         ┃Fetch Info:   ┃     ┃Validation Info:  ┃     ┃Items Loaded: ┃     |Total Rows:     |
#         ┃[]            ┃     ┃[]                ┃     ┃[]            ┃     |[]              |
#         ┗━━━━━━━━━━━━━━┛     ┗━━━━━━━━━━━━━━━━━━┛     ┗━━━━━━━━━━━━━━┛     +----------------+
#          Status: {test}             Status:                  Status:              Status:         

              
#     """
    
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)

#         # Init the animation variables so that the first frame can be rendered
#         self.test_start = "[-]"
#         self.arrow_start = "----►"
#         self.stage_index = 0
#         self.arrow_index_stage = 0

#     def compose(self) -> ComposeResult:

#         # Creating the Entry counter with caption
#         pi_group = Container(
#             Digits("3.141,592,653,5897", id="pi"),
#             Static("records added to the database", id="pi-caption"),
#             id="pi-group"
#         )
#         pi_group.border_title = "Database Entries"
#         yield pi_group

#         # Creating the diagram 
#         self.diagram_widget = Static(self.render_diagram(), id="diagram")
#         self.diagram_widget.border_title = "Pipeline Live Architecture"
#         yield self.diagram_widget

#     def render_diagram(self):
        
#         # Helper pad function
#         def pad(s, width=8):
#             return str(s)[:width].ljust(width)
        
#         # Make use of pythons fomratting to fill in placeholders
#         return self.diagram_template.format(
#             test=pad(self.test_start),
#             arr=pad(self.arrow_start)
#         )

#     def on_mount(self):
#         # call the animate method every second
#         self.set_interval(1, self.animate_status)

#     def animate_status(self):
#         stages = ["[+]", "[-]"]
#         arrow_stages = [" - ►", "- -- "]
#         self.test_start = stages[self.stage_index]
#         self.arrow_start = arrow_stages[self.arrow_index_stage]
#         self.stage_index = (self.stage_index + 1) % len(stages)
#         self.arrow_index_stage = (self.arrow_index_stage + 1) % len(arrow_stages)
#         self.diagram_widget.update(self.render_diagram())

class Overview(Vertical):
    diagram_template = """
        +-------------------+                                                                 
        |Data Source:       |                                                                 
        |TFL Disruptions API|                                                                 
        +-------------------+                                                                 
                |                                                                                 
                ▼                                                                                 
        ┏━━━━━━━┓━━━━━━┓     ┏━━━━━━━━━┓━━━━━━━━┓     ┏━━━━┓━━━━━━━━━┓     +----------------+
        ┃EXTRACT┃      ┃     ┃TRANSFORM┃        ┃     ┃LOAD┃         ┃     |Database:       |
        ┗━━━━━━━┛      ┃     ┗━━━━━━━━━┛        ┃     ┗━━━━┛         ┃     |                |
        ┃Last Fetch:   ┃     ┃Data Transformed: ┃     ┃Last Load:    ┃     |Last added rows:|
        ┃[$text-secondary]{last_fetch}[/]┃----►┃[$text-accent]{data_transformed}[/]    ┃----►┃[$text-success]{last_load}[/]┃----►|[$text-secondary]{last_added_rows}[/]  |
        ┃Fetch count:  ┃     ┃Fields Cleaned:   ┃     ┃Items Loaded: ┃     |Total Rows:     |
        ┃[$text-accent]{fetch_count}[/]┃     ┃[$text-success]{fields_stripped}[/]    ┃     ┃[$text-accent]{items_loaded}[/]┃     |[$text-success]{total_rows}[/]  |
        ┗━━━━━━━━━━━━━━┛     ┗━━━━━━━━━━━━━━━━━━┛     ┗━━━━━━━━━━━━━━┛     +----------------+
         Status:              Status:                  Status:              Status:         
    """

    def compose(self) -> ComposeResult:

        # Creating the Entry counter with caption
        pi_group = Container(
            Digits("0", id="pi"),
            Static("records added to the database", id="pi-caption"),
            id="pi-group"
        )
        pi_group.border_title = "Database Entries"
        yield pi_group

        # Creating the diagram 
        self.diagram_widget = Static(self.diagram_template, id="diagram")
        self.diagram_widget.border_title = "Pipeline Live Architecture"
        yield self.diagram_widget

    def on_mount(self):
        # Run an update loop to get UI updates
        self.set_interval(1, self.update_view)

    def update_view(self):
        # Try to read the data from the last_saved_info file to update the UI
        file_location = f"{shared_logger.logs_location}/{shared_logger.last_run_info_filename}"

        # little helper pad function
        def pad(s, width=14):
            return str(s)[:width].ljust(width)

        try:
            with open(file_location, "r") as f:
                data = json.load(f)
                last_fetched = data['Last-fetch']
                fetch_count = data['Fetch-count']
                data_transformed = data['Data-transformed']
                fields_stripped = data['Fields-stripped']
                last_load = data["Last-load"]
                items_loaded = data["Items-loaded"]
                last_added_rows = data["Last-added-rows"]
                total_rows = data["Total-rows"]

                # update the view
                diagram = self.diagram_template.format(
                    last_fetch=pad(last_fetched),
                    fetch_count=pad(fetch_count),
                    data_transformed=pad(data_transformed),
                    fields_stripped=pad(fields_stripped),
                    last_load=pad(last_load),
                    items_loaded=pad(items_loaded),
                    last_added_rows=pad(last_added_rows),
                    total_rows=pad(total_rows)
                )
                
                # update digits display
                self.query_one("#pi", Digits).update(str(total_rows))

            self.diagram_widget.update(diagram)
        except Exception:
            pass


class PipelineTUI(App):
    
    CSS = """
    Screen {
        align: center middle;
    }
    #pi-group {
        width: auto;
        border: round $primary;
        align: center middle;
        background: $primary-muted;
    }
    #diagram {
        border: round white;
        text-wrap: nowrap;
        overflow: auto;
    }
    """

    BINDINGS = [
        Binding(key="^q", action="quit", description="Quit"),
    ]

    def compose(self) -> ComposeResult:

        # Header element
        yield Header()

        # Some tabs
        yield Tabs(
            Tab("Overview", id="one"),
            Tab("Pipeline Control", id="two"),
            Tab("Configuration", id="three"),
            Tab("Alerts & Notifications", id="four"),
            Tab("Audit", id="five"),
            Tab("Operation Logs", id="six"),
        )

        # Containers for the tabs
        yield Container(
            Center(Overview(id="content-one")),
            Center(Static("Pipeline Control content", id="content-two")),
            Center(Static("Configuration content", id="content-three")),
            Center(Static("Alerts & Notifications content", id="content-four")),
            Center(Static("Audit content", id="content-five")),
            Center(Static("Operation Logs content", id="content-six")),
            id="tab-content"
        )

        # Footer element
        yield Footer()

    def on_mount(self) -> None:

        # General Style
        self.theme = "monokai"

        # Header Fields
        self.title = "Pipeline Control Center"
        self.sub_title = "London Traffic ETL"
        header = self.query_one(Header)
        header.tall = True
        header._show_clock = True
        header.icon = "[+]"

    def on_tabs_tab_activated(self, event) -> None:
        # Hide all content widgets, show only the active one
        for tab_id in ["one", "two", "three", "four", "five", "six"]:
            content = self.query_one(f"#content-{tab_id}")
            content.display = (event.tab.id == tab_id)


if __name__ == "__main__":
    app = PipelineTUI()
    app.run()