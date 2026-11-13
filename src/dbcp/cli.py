"""A Command line interface for the down ballot project."""

import logging
from pathlib import Path

import click
import coloredlogs
from jinja2 import Environment, PackageLoader, select_autoescape

import dbcp
from dbcp.commands.publish import inspect_outputs, publish_outputs, upload_outputs
from dbcp.metadata import SchemaName
from dbcp.metadata.data_mart import Base as DataMartBase
from dbcp.metadata.data_warehouse import Base as DataWarehouseBase
from dbcp.transform.fips_tables import SPATIAL_CACHE
from dbcp.transform.helpers import GEOCODER_CACHES

logger = logging.getLogger(__name__)


@click.group(name="dbcp")
@click.option(
    "--loglevel",
    help="Set logging level (DEBUG, INFO, WARNING, ERROR, or CRITICAL).",
    default="INFO",
)
def cli(loglevel):
    """A command line interface for the down ballot project."""
    dbcp_logger = logging.getLogger()
    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    coloredlogs.install(fmt=log_format, level=loglevel, logger=dbcp_logger)


@cli.command()
@click.option(
    "-dm",
    "--data-mart",
    help="Load the data mart tables to the database",
    default=False,
    is_flag=True,
)
@click.option(
    "-dw",
    "--data-warehouse",
    help="Load the data warehouse tables to the database",
    default=False,
    is_flag=True,
)
@click.option(
    "-clr",
    "--clear-cache",
    help="Delete saved geocoder and spatial join results, forcing fresh API calls and computation.",
    default=False,
    is_flag=True,
)
def etl(data_mart: bool, data_warehouse: bool, clear_cache: bool):
    """Run the ETL process to produce the data warehouse and mart."""
    if clear_cache:
        GEOCODER_CACHES.clear_caches()
        SPATIAL_CACHE.clear()

    if data_warehouse:
        dbcp.etl.etl(schema=SchemaName("data_warehouse"))
    if data_mart:
        dbcp.etl.etl(schema=SchemaName("data_mart"))
    if (not data_warehouse) and (not data_mart):
        raise ValueError(
            "Please specify a target for the ETL process: --data-warehouse and/or --data-mart."
        )


def _iter_models(base):
    for mapper in base.registry.mappers:
        yield mapper.class_


@cli.command()
@click.argument("output_dir", type=click.Path(path_type=Path))
def render_table_docs(output_dir: Path):
    """Render markdown documentation for all SQLAlchemy tables."""
    env = Environment(
        loader=PackageLoader("dbcp.metadata", "templates"),
        autoescape=select_autoescape(enabled_extensions=("html", "xml", "md")),
    )

    # Render zensical.toml config
    zensical_template = env.get_template("zensical.toml.j2")
    data_mart_tables = [model.__table__.name for model in _iter_models(DataMartBase)]
    data_warehouse_tables = [
        model.__table__.name for model in _iter_models(DataWarehouseBase)
    ]
    zensical_output_path = Path(__file__).resolve().parents[2] / "zensical.toml"
    zensical_output_path.write_text(
        zensical_template.render(
            data_mart_tables=sorted(data_mart_tables),
            data_warehouse_tables=sorted(data_warehouse_tables),
        ),
        encoding="utf-8",
    )

    # Render markdown for each table
    table_template = env.get_template("table_template.md.j2")
    for schema_name, base in (
        ("data_mart", DataMartBase),
        ("data_warehouse", DataWarehouseBase),
    ):
        schema_dir = output_dir / schema_name
        schema_dir.mkdir(parents=True, exist_ok=True)

        for model in _iter_models(base):
            table_name = model.__table__.name
            output_path = schema_dir / f"{table_name}.md"
            output_path.write_text(table_template.render(model=model), encoding="utf-8")


cli.add_command(publish_outputs)
cli.add_command(inspect_outputs)
cli.add_command(upload_outputs)
cli.add_command(render_table_docs)

if __name__ == "__main__":
    cli()
