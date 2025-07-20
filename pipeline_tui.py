"""
Julyan van der Westhuizen
20/07/25

This is a Terminal User Interface for interacting with the pipeline.
"""

from textual.app import App, ComposeResult
from textual.containers import Container, Center
from textual.binding import Binding
from textual.widgets import Tabs, Tab, Header, Static, Footer, Digits
from textual.widget import Widget
from textual.containers import Vertical

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
        ┃[]            ┃----►┃[]                ┃----►┃[]            ┃----►|[]              |
        ┃Fetch Info:   ┃     ┃Validation Info:  ┃     ┃Items Loaded: ┃     |Total Rows:     |
        ┃[]            ┃     ┃[]                ┃     ┃[]            ┃     |[]              |
        ┗━━━━━━━━━━━━━━┛     ┗━━━━━━━━━━━━━━━━━━┛     ┗━━━━━━━━━━━━━━┛     +----------------+
         Status:{extract_status}             Status:{transform_status}         Status:{load_status}         Status:{db_status}                                
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Init the animations variables so that the first frame can be rendered
        self.extract_status = "Idle"
        self.transform_status = "Idle"
        self.load_status = "Idle"
        self.db_status = "Idle"

    def compose(self) -> ComposeResult:

        # Creating the Entry counter with caption
        pi_group = Container(
            Digits("3.141,592,653,5897", id="pi"),
            Static("records added to the database", id="pi-caption"),
            id="pi-group"
        )
        pi_group.border_title = "Database Entries"
        yield pi_group

        # Creating the diagram 
        self.diagram_widget = Static(self.render_diagram(), id="diagram")
        self.diagram_widget.border_title = "Pipeline Live Architecture"
        yield self.diagram_widget

    def render_diagram(self):
        def pad(s, width=8):
            return str(s)[:width].ljust(width)
        return self.diagram_template.format(
            extract_status=pad(self.extract_status),
            transform_status=pad(self.transform_status),
            load_status=pad(self.load_status),
            db_status=pad(self.db_status)
        )

    def on_mount(self):
        # Example: animate extract_status every second
        self.set_interval(1, self.animate_status)

    def animate_status(self):
        # Cycle through some example statuses
        import random
        statuses = ["Idle", "Running", "Success", "Error"]
        self.extract_status = random.choice(statuses)
        self.diagram_widget.update(self.render_diagram())

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

        
        # Switching between tabs
    def on_tabs_tab_activated(self, event) -> None:
        # Hide all content widgets, show only the active one
        for tab_id in ["one", "two", "three", "four", "five", "six"]:
            content = self.query_one(f"#content-{tab_id}")
            content.display = (event.tab.id == tab_id)


if __name__ == "__main__":
    app = PipelineTUI()
    app.run()