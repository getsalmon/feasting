import asyncio
import logging
import sys

import click

import consumer
from config import load_config


def setup_logging(verbose: bool = False):
    """Configure logging for the application"""
    log_level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    # Set specific loggers
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("kafka").setLevel(logging.WARNING)


@click.group(invoke_without_command=True)
@click.option("--config", default=None, help="Config file path")
@click.option("--kafka-host", default=None, help="Kafka host")
@click.option("--kafka-port", type=int, default=None, help="Kafka port")
@click.option("--kafka-topic", type=str, default=None, help="Kafka topic")
@click.option("--pg-host", default=None, help="Postgres host")
@click.option("--pg-port", type=int, default=None, help="Postgres port")
@click.option("--pg-user", type=str, default=None, help="Postgres user")
@click.option("--pg-password", type=str, default=None, help="Postgres password")
@click.option("--pg-dbname", type=str, default=None, help="Postgres dbname")
@click.option("--verbose", type=bool, default=None, is_flag=True)
@click.pass_context
def cli(
    ctx,
    config: str,
    kafka_host: str,
    kafka_port: int,
    kafka_topic: str,
    pg_host: str,
    pg_port: int,
    pg_user: str,
    pg_password: str,
    pg_dbname: str,
    verbose: bool,
):
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())
        sys.exit(0)

    cfg = load_config(config)
    ctx.ensure_object(dict)
    ctx.obj["kafka_host"] = (
        kafka_host if kafka_host is not None else cfg.get("kafka", {}).get("host")
    )
    ctx.obj["kafka_port"] = (
        kafka_port if kafka_port is not None else cfg.get("kafka", {}).get("port")
    )
    ctx.obj["kafka_topic"] = (
        kafka_topic if kafka_topic is not None else cfg.get("kafka", {}).get("topic")
    )

    ctx.obj["pg_host"] = (
        pg_host if pg_host is not None else cfg.get("postgres", {}).get("host")
    )
    ctx.obj["pg_port"] = (
        pg_port if pg_port is not None else cfg.get("postgres", {}).get("port")
    )
    ctx.obj["pg_user"] = (
        pg_user if pg_user is not None else cfg.get("postgres", {}).get("user")
    )
    ctx.obj["pg_password"] = (
        pg_password
        if pg_password is not None
        else cfg.get("postgres", {}).get("password")
    )
    ctx.obj["pg_dbname"] = (
        pg_dbname if pg_dbname is not None else cfg.get("postgres", {}).get("dbname")
    )

    ctx.obj["verbose"] = verbose if verbose is not None else cfg.get("verbose")


@cli.command()
@click.pass_context
def consume(ctx):
    """Start consuming from kafka"""
    kafka_host = ctx.obj["kafka_host"]
    kafka_port = ctx.obj["kafka_port"]
    kafka_topic = ctx.obj["kafka_topic"]
    verbose = ctx.obj["verbose"]
    pg_host = ctx.obj["pg_host"]
    pg_port = ctx.obj["pg_port"]
    pg_user = ctx.obj["pg_user"]
    pg_password = ctx.obj["pg_password"]

    # Setup logging
    setup_logging(verbose)

    click.echo(f"consuming from server on {kafka_host}:{kafka_port}@{kafka_topic}")
    click.echo(f"pg on {pg_user}:{pg_password}@{pg_host}:{pg_port} ")
    asyncio.run(consumer.consume(kafka_host, kafka_port, kafka_topic, verbose))


@cli.command()
@click.pass_context
def print_config(ctx):
    """Print current config"""
    kafka_host = ctx.obj["kafka_host"]
    kafka_port = ctx.obj["kafka_port"]
    kafka_topic = ctx.obj["kafka_topic"]
    pg_host = ctx.obj["pg_host"]
    pg_port = ctx.obj["pg_port"]
    pg_user = ctx.obj["pg_user"]
    pg_password = ctx.obj["pg_password"]
    pg_dbname = ctx.obj["pg_dbname"]
    verbose = ctx.obj["verbose"]

    click.echo(f"consuming from server on {kafka_host}:{kafka_port}@{kafka_topic}")
    click.echo(f"pg on {pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_dbname}")
    click.echo(f"verbosity if {'on' if verbose else 'off'}")


if __name__ == "__main__":
    cli()
