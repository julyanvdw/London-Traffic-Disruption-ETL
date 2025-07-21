"""
Julyan van der Westhuizen
20/07/25

This is a Terminal User Interface for interacting with the pipeline.
"""

from pipeline_log_manager import shared_logger
import json
import os
from textual.app import App, ComposeResult
from textual.containers import Container, Center, Horizontal
from textual.binding import Binding
from textual.widgets import Tabs, Tab, Header, Static, Footer, Digits, Log, Label, Button, Switch
from textual.containers import Vertical
from extract import fetch_TIMS
from transform import transformer
from load import loader
import pipeline_orchestrator


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
        ┃[$text-success]{last_fetch}[/]┃----►┃[$text-accent]{data_transformed}[/]    ┃----►┃[$text-success]{last_load}[/]┃----►|[$text-secondary]{last_added_rows}[/]  |
        ┃Fetch count:  ┃     ┃Fields Cleaned:   ┃     ┃Items Loaded: ┃     |Total Rows:     |
        ┃[$text-accent]{fetch_count}[/]┃     ┃[$text-success]{fields_stripped}[/]    ┃     ┃[$text-secondary]{items_loaded}[/]┃     |[$text-success]{total_rows}[/]  |
        ┗━━━━━━━━━━━━━━┛     ┗━━━━━━━━━━━━━━━━━━┛     ┗━━━━━━━━━━━━━━┛     +----------------+
         Status:{extract_status}Status:{transform_status}    Status:{load_status}Status:{database_status}         
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

                extract_status = data["Extract-status"]
                transform_status = data["Transform-status"]
                load_status = data["Load-status"]
                database_status = data["Database-status"]

                # Status set up
                if extract_status == 0:
                    extract_status_msg = " OK [*]"
                elif extract_status == 1:
                    extract_status_msg = " ERROR [!]"

                if transform_status == 0:
                    transform_status_msg = " OK [*]"
                elif transform_status == 1:
                    transform_status_msg = " ERROR [!]"

                if load_status == 0:
                    load_status_msg = " OK [*]"
                elif load_status == 1:
                    load_status_msg = " ERROR [!]"

                if database_status == 0:
                    database_status_msg = " OK [*]"
                elif database_status == 1:
                    database_status_msg = " ERROR [!]"
                                

                # update the view
                diagram = self.diagram_template.format(
                    last_fetch=pad(last_fetched),
                    fetch_count=pad(fetch_count),
                    data_transformed=pad(data_transformed),
                    fields_stripped=pad(fields_stripped),
                    last_load=pad(last_load),
                    items_loaded=pad(items_loaded),
                    last_added_rows=pad(last_added_rows),
                    total_rows=pad(total_rows),
                    extract_status=pad(extract_status_msg),
                    transform_status=pad(transform_status_msg),
                    load_status=pad(load_status_msg),
                    database_status=pad(database_status_msg)
                )
                
                # update digits display
                self.query_one("#pi", Digits).update(str(total_rows))

            self.diagram_widget.update(diagram)
        except Exception:
            pass

class LogsView(Vertical):
    
    def compose(self) -> ComposeResult:
        yield Log(highlight=True, auto_scroll=True)
    
    def on_mount(self):
        self.set_interval(1, self.update_log)  # refresh every second

    def update_log(self):
        log = self.query_one(Log)
        
        # Get the filename from sharedlogger
        filename = f"{shared_logger.logs_location}/{shared_logger.logs_filename}"
        log.clear()

        with open(filename, 'r') as f:
            for line in f:
                log.write_line(line.rstrip())

class PipelineControl(Vertical):
    
    def compose(self) -> ComposeResult:
        
        # Creating three buttons for each stage of the pipeline - for manual running
        group1 = Container(
            Label("Run EXTRACTION Manually", id="control-label-1"),
            Button("Extract", id="control-button-1", variant="warning"),
            id="control-group-1"
        )
        group1.border_title = "EXTRACT"
        group1.border_style = "round"

        group2 = Container(
            Label("Run TRANSFORM Manually", id="control-label-2"),
            Button("Transform", id="control-button-2", variant="warning"),
            id="control-group-2"
        )
        group2.border_title = "TRANSFORM"
        group2.border_style = "round"

        group3 = Container(
            Label("Run LOAD Manually", id="control-label-3"),
            Button("Load", id="control-button-3", variant="warning"),
            id="control-group-3"
        )
        group3.border_title = "LOAD"
        group3.border_style = "round"

        # Creating a button for entire pipeline running
        group_full = Container(
            Label("Run FULL PIPELINE Manually", id="control-label-4"),
            Button("Run Pipeline", id="control-button-4", variant="primary"),
            id="control-group-4"
        )
        group_full.border_title = "PIPELINE"
        group_full.border_style = "round"

        # Create a toggle switch group with border
        group_toggle = Container(
            Label("Activate / Deactivate Pipeline", id="toggle-label"),
            Switch(id="toggle-switch", value=True),
            id="toggle-group"
        )
        group_toggle.border_title = "MODE"
        group_toggle.border_style = "round"

        # Horizontal container for group_full and toggle switch group
        yield Horizontal(
            group_toggle,
            group_full,
            id="full-controls-horizontal"
        )

        # Horizontal container for the three stage groups
        yield Horizontal(
            group1,
            group2,
            group3,
            id="controls-horizontal"
        )

    def on_button_pressed(self, event: Button.Pressed):
        if event.button.id == "control-button-1":
            # Manually run the extraction
            fetch_TIMS.fetch_tims_data()

        elif event.button.id == "control-button-2":
            # Manually run the transformer - ingest step 
            transformer.ingest()

        elif event.button.id == "control-button-3":
            # Manually load data into the DB
            loader.load()

        elif event.button.id == "control-button-4":
            # Manually run the full orchestration 
            pipeline_orchestrator.run_pipeline()

    def on_switch_changed(self, event: Switch.Changed):
        # Get the directory of this script
        path = os.path.dirname(os.path.abspath(__file__))
        flag_path = f"{path}/pipeline_enabled.flag"
        if event.value:
            # Create the flag - essentially enabling the pipeline 
            open(flag_path, "w").close()
        else:
            # Remove the flag - disable the pipeline
            if os.path.exists(flag_path):
                os.remove(flag_path)


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
    #controls-horizontal {
        width: 100%;
        align: center middle;
        margin: 2;
    }
    #full-controls-horizontal {
        background: $primary-muted;
        width: 100%;
        align: center middle;
        margin: 2;
    }
    #control-group-1, #control-group-2, #control-group-3, #control-group-4, #toggle-group {
        width: 1fr;
        min-width: 20;
        height: auto;
        border: round white;
        align: center middle;
        padding: 1;
        margin: 1;
    }
    #toggle-group {
        background: $accent-muted;
        border: round $accent;
        margin-left: 1;
        margin-right: 1;
        margin-top: 1;
        padding: 1;
    }
    #toggle-label {
        margin-bottom: 1;
    }
    #toggle-switch {
        margin-top: 1;
    }
    #control-label-1, #control-label-2, #control-label-3, #control-label-4 {
        margin-bottom: 1;
    }
    #control-button-1, #control-button-2, #control-button-3, #control-button-4 {
        margin-top: 1;
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
            Tab("Audit", id="three"),
            Tab("Operation Logs", id="four"),
        )

        # Containers for the tabs
        yield Container(
            Center(Overview(id="content-one")),
            Center(PipelineControl(id="content-two")),
            Center(Static("Audit content", id="content-three")),
            Center(LogsView(id="content-four")),
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
        for tab_id in ["one", "two", "three", "four"]:
            content = self.query_one(f"#content-{tab_id}")
            content.display = (event.tab.id == tab_id)


if __name__ == "__main__":
    app = PipelineTUI()
    app.run()