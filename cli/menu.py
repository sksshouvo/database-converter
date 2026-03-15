"""Rich interactive menu for the Universal Database Converter CLI."""
from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, IntPrompt, Prompt
from rich.table import Table
from rich import box

from models.database_config import DatabaseConfig
from services.migration_service import MigrationConfig, MigrationService
from services.scheduler_service import SchedulerService
from storage.control_db import get_session
from models.migration_job import MigrationJob
from utils.logger import get_logger

console = Console()
logger = get_logger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _print_header() -> None:
    console.print(
        Panel.fit(
            "[bold cyan]Universal Database Converter CLI[/bold cyan]\n"
            "[dim]MySQL · MSSQL · PostgreSQL[/dim]",
            border_style="cyan",
        )
    )


def _prompt_db_config(label: str) -> DatabaseConfig:
    """Interactively prompt for database connection details."""
    console.rule(f"[bold]{label} Database[/bold]")
    from core.connectors.connector_factory import ConnectorFactory
    engines = ConnectorFactory.supported_engines()
    engine = Prompt.ask(
        f"Engine",
        choices=engines,
        default="mysql",
    )
    host = Prompt.ask("Host", default="localhost")
    port_defaults = {"mysql": 3306, "mssql": 1433, "postgresql": 5432}
    port = IntPrompt.ask("Port", default=port_defaults.get(engine, 5432))
    username = Prompt.ask("Username")
    password = Prompt.ask("Password", password=True)
    database = Prompt.ask("Database name (leave blank for discovery)", default="")
    return DatabaseConfig(
        engine=engine,
        host=host,
        port=port,
        username=username,
        password=password,
        database=database or None,
    )


def _show_jobs_table(jobs: list) -> None:
    if not jobs:
        console.print("[yellow]No migration jobs found.[/yellow]")
        return
    table = Table(title="Migration Jobs", box=box.ROUNDED)
    table.add_column("Job ID", style="dim", max_width=36)
    table.add_column("Table", style="white")
    table.add_column("Status", style="bold")
    table.add_column("Progress", justify="right")
    table.add_column("Source", style="dim")
    table.add_column("Destination", style="dim")

    for job in jobs:
        status_color = {
            "completed": "green",
            "running": "cyan",
            "failed": "red",
            "pending": "yellow",
            "paused": "magenta",
        }.get(job.status, "white")

        table.add_row(
            job.job_id[:8] + "…",
            job.table_name,
            f"[{status_color}]{job.status}[/{status_color}]",
            f"{job.progress_pct}%",
            f"{job.source_engine}@{job.source_host}/{job.source_database}",
            f"{job.destination_engine}@{job.destination_host}/{job.destination_database}",
        )
    console.print(table)


# ── Menu screens ──────────────────────────────────────────────────────────────

def screen_create_migration() -> None:
    """Wizard for a new migration job."""
    console.rule("[bold green]Create New Migration[/bold green]")
    src_cfg = _prompt_db_config("Source")
    dst_cfg = _prompt_db_config("Destination")

    from core.validators.connection_validator import validate_connection
    with console.status("Testing connections…"):
        for label, cfg in [("Source", src_cfg), ("Destination", dst_cfg)]:
            ok, err = validate_connection(cfg)
            if not ok:
                console.print(f"[red]❌ {label} connection failed: {err}[/red]")
                return
    console.print("[green]✅ Both connections valid.[/green]")

    tables_input = Prompt.ask(
        "Tables to migrate (comma-separated, blank = all)", default=""
    )
    tables = [t.strip() for t in tables_input.split(",") if t.strip()] or None

    workers = IntPrompt.ask("Max parallel workers", default=4)
    batch = IntPrompt.ask("Batch size (rows)", default=1000)
    run_val = Confirm.ask("Run post-migration validation?", default=True)

    dst_cfg.database = dst_cfg.database or src_cfg.database

    migration_cfg = MigrationConfig(
        source=src_cfg,
        destination=dst_cfg,
        tables=tables,
        max_workers=workers,
        batch_size=batch,
        run_validation=run_val,
    )

    if not Confirm.ask("\n[bold]Start migration now?[/bold]", default=True):
        console.print("[yellow]Aborted.[/yellow]")
        return

    with console.status("[cyan]Running migration…[/cyan]", spinner="dots"):
        try:
            svc = MigrationService()
            jobs = svc.run(migration_cfg)
        except Exception as exc:
            console.print(f"[red]Migration error: {exc}[/red]")
            logger.error(f"Migration failed: {exc}", exc_info=True)
            return

    completed = sum(1 for j in jobs.values() if j.status == "completed")
    console.print(
        f"\n[green]✅ Migration done — {completed}/{len(jobs)} tables completed.[/green]"
    )


def screen_view_jobs() -> None:
    """Display all migration jobs from the control DB."""
    console.rule("[bold blue]Migration Jobs[/bold blue]")
    with get_session() as session:
        from sqlalchemy import select
        jobs = session.execute(select(MigrationJob)).scalars().all()
        jobs = list(jobs)
    _show_jobs_table(jobs)


def screen_schedule_job() -> None:
    """Schedule an existing job for recurring execution."""
    console.rule("[bold magenta]Schedule a Job[/bold magenta]")
    job_id = Prompt.ask("Enter Job ID to schedule")
    cron_expr = Prompt.ask("Cron expression (e.g. '0 2 * * *' for 2 AM daily)")

    svc = SchedulerService()
    try:
        sj = svc.create_scheduled_job(job_id, cron_expr)
        console.print(
            f"[green]✅ Scheduled: {sj.schedule_id} | '{cron_expr}'[/green]"
        )
    except Exception as exc:
        console.print(f"[red]Scheduling failed: {exc}[/red]")


def screen_stop_job() -> None:
    """Stop a scheduled job."""
    console.rule("[bold yellow]Stop Scheduled Job[/bold yellow]")
    svc = SchedulerService()
    jobs = svc.list_scheduled_jobs()
    if not jobs:
        console.print("[yellow]No active scheduled jobs.[/yellow]")
        return
    for sj in jobs:
        console.print(f"  [{sj.schedule_id}] {sj.interval_expr} → job {sj.job_id} ({sj.status})")
    schedule_id = Prompt.ask("Schedule ID to stop")
    if svc.stop_scheduled_job(schedule_id):
        console.print("[green]✅ Scheduled job stopped.[/green]")
    else:
        console.print("[red]Schedule ID not found.[/red]")


def screen_view_status() -> None:
    """Show detailed status for a specific job."""
    console.rule("[bold]Job Status[/bold]")
    job_id = Prompt.ask("Enter full Job ID")
    with get_session() as session:
        job = session.get(MigrationJob, job_id)
    if not job:
        console.print("[red]Job not found.[/red]")
        return
    console.print(
        Panel(
            f"[bold]Job ID:[/bold]     {job.job_id}\n"
            f"[bold]Table:[/bold]      {job.table_name}\n"
            f"[bold]Status:[/bold]     {job.status}\n"
            f"[bold]Progress:[/bold]   {job.converted_rows}/{job.total_rows} rows ({job.progress_pct}%)\n"
            f"[bold]Source:[/bold]     {job.source_engine}@{job.source_host}/{job.source_database}\n"
            f"[bold]Dest:[/bold]       {job.destination_engine}@{job.destination_host}/{job.destination_database}\n"
            f"[bold]Batch size:[/bold] {job.batch_size}\n"
            f"[bold]Error:[/bold]      {job.error_message or '—'}\n"
            f"[bold]Created:[/bold]    {job.created_at}\n"
            f"[bold]Updated:[/bold]    {job.updated_at}",
            title="Job Details",
            border_style="blue",
        )
    )


# ── Main menu loop ────────────────────────────────────────────────────────────

class Menu:
    """Interactive console menu controller."""

    ITEMS = [
        ("1", "Create Migration",       screen_create_migration),
        ("2", "View Migration Jobs",     screen_view_jobs),
        ("3", "Schedule a Job",          screen_schedule_job),
        ("4", "Stop Scheduled Job",      screen_stop_job),
        ("5", "View Job Status",         screen_view_status),
        ("6", "Exit",                    None),
    ]

    def run(self) -> None:
        """Block until the user selects Exit."""
        while True:
            console.print()
            _print_header()
            for key, label, _ in self.ITEMS:
                console.print(f"  [bold cyan]{key}[/bold cyan]. {label}")
            console.print()
            choice = Prompt.ask(
                "Select an option",
                choices=[k for k, _, _ in self.ITEMS],
                default="1",
            )
            for key, label, handler in self.ITEMS:
                if key == choice:
                    if handler is None:
                        console.print("\n[bold]Goodbye! 👋[/bold]\n")
                        return
                    console.print()
                    try:
                        handler()
                    except KeyboardInterrupt:
                        console.print("\n[yellow]Interrupted.[/yellow]")
                    break
