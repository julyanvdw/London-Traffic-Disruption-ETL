"""
Julyan van der Westhuizen
20/07/25

This is a Terminal User Interface for interacting with the pipeline.
"""

from textual.app import App, ComposeResult
from textual.containers import Container, Center
from textual.binding import Binding
from textual.widgets import Tabs, Tab, Header, Static, Footer

class Overview(Static):
    
    pipeline_diagram = """
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
        ┃[]            ┃----►┃[]                ┃----►┃[]            ┃----►|[]              |
        ┃Fetch Info:   ┃     ┃Validation Info:  ┃     ┃Items Loaded: ┃     |Total Rows:     |
        ┃[]            ┃     ┃[]                ┃     ┃[]            ┃     |[]              |
        ┗━━━━━━━━━━━━━━┛     ┗━━━━━━━━━━━━━━━━━━┛     ┗━━━━━━━━━━━━━━┛     +----------------+
                                                                                            
        """

    def on_mount(self):
        self.update(self.pipeline_diagram)

class PipelineTUI(App):

    BINDINGS = [
        Binding(key="^q", action="quit", description="Quit"),
    ]

    def compose(self) -> ComposeResult:

        yield Header()

        yield Tabs(
            Tab("Overview", id="one"),
            Tab("Pipeline Control", id="two"),
            Tab("Configuration", id="three"),
            Tab("Alerts & Notifications", id="four"),
            Tab("Audit", id="five"),
            Tab("Operation Logs", id="six"),
        )

        yield Container(
            Center(Overview(id="content-one")),
            Center(Static("Pipeline Control content", id="content-two")),
            Center(Static("Configuration content", id="content-three")),
            Center(Static("Alerts & Notifications content", id="content-four")),
            Center(Static("Audit content", id="content-five")),
            Center(Static("Operation Logs content", id="content-six")),
            id="tab-content"
        )

        yield Footer()

    def on_mount(self) -> None:

        # General Style
        self.theme = "flexoki"

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
            content = self.query_one(f"#content-{tab_id}", Static)
            content.display = (event.tab.id == tab_id)


if __name__ == "__main__":
    app = PipelineTUI()
    app.run()