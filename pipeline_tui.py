"""
Julyan van der Westhuizen
20/07/25

This is a Terminal User Interface for interacting with the pipeline.
"""

from textual.app import App, ComposeResult
from textual.containers import Container
from textual.widgets import Tabs, Tab, Header, Static


class PipelineTUI(App):

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
            Static("Overview content", id="content-one"),
            Static("Pipeline Control content", id="content-two"),
            Static("Configuration content", id="content-three"),
            Static("Alerts & Notifications content", id="content-four"),
            Static("Audit content", id="content-five"),
            Static("Operation Logs content", id="content-six"),
            id="tab-content"
        )

    def on_mount(self) -> None:
        self.title = "Pipeline Control Center"
        self.sub_title = "London Traffic ETL"

    def on_tabs_tab_activated(self, event) -> None:
        # Hide all content widgets, show only the active one
        for tab_id in ["one", "two", "three", "four", "five", "six"]:
            content = self.query_one(f"#content-{tab_id}", Static)
            content.display = (event.tab.id == tab_id)



if __name__ == "__main__":
    app = PipelineTUI()
    app.run()