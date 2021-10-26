"""
RabbitMQ Recoverer is a tool to extract messages from RabbitMQ disk storage.
It can be used in case after a crash, Mnesia logs corruption, or for forensic needs.
"""
from typing import List, Tuple
import pathlib
import typer
from msg_store import parse_rdq_file
from msg_index import parse_idx_file
from msg_exporter import dump_message

app = typer.Typer()


def _recover_rdq_file(rdq_file_path: pathlib.Path, output_directory: pathlib.Path, verbose: bool = False) -> int:
    """
    Private function for command recover-rdq-files. Takes pathlib.Path objects as arguments instead of strings.

    :param rdq_file_path: path to .rdq file
    :param output_directory: directory extracted messages will be written to
    :param verbose: define log level for this function. Useful when called through recover-persistent-store
    """
    # Check file extension and existency
    assert rdq_file_path.suffix.lower() == ".rdq"
    assert rdq_file_path.exists()
    assert rdq_file_path.is_file()

    # Create output directory if it doesn't exist
    output_directory.mkdir(mode=0o755, exist_ok=True)

    if verbose:
        typer.secho(
            f"Recovering {rdq_file_path}.",
            fg=typer.colors.BLUE,
        )
    messages = parse_rdq_file(rdq_file_path)

    # Dump messages
    for message in messages:
        dump_message(message, output_directory)

    if verbose:
        typer.secho(f"Recovered {len(messages)} messages.", fg=typer.colors.BLUE)

    return len(messages)


def _recover_persistent_store(store_path: pathlib.Path, output_directory: pathlib.Path):
    """
    Private function for command recover-persistent-store. Takes pathlib.Path objects as arguments instead of strings.

    :param store_path: path to the msg_store_persistent directory containing .rdq files
    :param output_directory: directory extracted messages will be written to
    """
    # Check directory existency
    assert store_path.exists()
    assert store_path.is_dir()

    # List all RDQ files in store
    rdq_files = list(store_path.glob("*.rdq"))
    typer.secho(f"Found {len(rdq_files)} files to recover.", fg=typer.colors.CYAN)

    # Recover each file one by one
    total_messages_recovered = 0
    with typer.progressbar(rdq_files) as files:
        for file in files:
            messages_in_file = _recover_rdq_file(file, output_directory)
            total_messages_recovered += messages_in_file

    typer.secho(f"Recovered {total_messages_recovered} messages.", fg=typer.colors.CYAN)


def _recover_idx_file(idx_file_path: pathlib.Path, output_directory: pathlib.Path, verbose: bool = False) -> int:
    """
    Private function for command recover-messages-stored-in-idx-file. Takes pathlib.Path objects as arguments instead of strings.

    :param idx_file_path: path to the .idx file
    :param output_directory: directory extracted messages will be written to
    :param verbose: define log level for this function. Useful when called through recover-persistent-store
    :return: counter of recovered unacked messages
    """
    assert idx_file_path.suffix.lower() == ".idx"
    assert idx_file_path.exists()
    assert idx_file_path.is_file()

    if verbose:
        typer.secho(
            f"Recovering {idx_file_path}.",
            fg=typer.colors.BLUE,
        )

    messages = parse_idx_file(idx_file_path)

    for message in messages:
        dump_message(message, output_directory)

    if verbose:
        typer.secho(
            f"Recovered {len(messages)} messages.",
            fg=typer.colors.BLUE,
        )

    return len(messages)


def _recover_indexes(indexes_path: pathlib.Path, output_directory: pathlib.Path):
    """
    Private function for command recover_indexes_and_prune_acked. Takes pathlib.Path objects as arguments instead of strings.

    :param indexes_path: path to the queues directory containing all /id/*.idx files
    :param output_directory: path extracted messages will be written to
    """
    # Check directory existency
    assert indexes_path.exists()
    assert indexes_path.is_dir()

    # List all IDX files in directory
    idx_files = list(indexes_path.glob("./*/*.idx"))
    typer.secho(f"Found {len(idx_files)} files to recover.", fg=typer.colors.CYAN)

    # Recover each index one by one
    total_messages_recovered = 0
    with typer.progressbar(idx_files) as files:
        for file in files:
            messages_recovered_count = _recover_idx_file(file, output_directory)
            total_messages_recovered += messages_recovered_count

    typer.secho(f"Recovered {total_messages_recovered} messages.", fg=typer.colors.CYAN)


@app.command()
def recover_rdq_file(filepath: str, output_dir: str):
    """
    Command to extract messages from a specific .rdq file.

    :param filepath: path to .rdq file
    :param output_dir: directory extracted messages will be written to
    """
    input_path = pathlib.Path(filepath)
    output_path = pathlib.Path(output_dir)
    _recover_rdq_file(input_path, output_path, verbose=True)


@app.command()
def recover_idx_file(filepath: str, output_dir: str):
    """
    Command to extract unacked messages from a specific .idx.

    :param filepath: path to .idx file
    :param output_dir: directory extracted records will be written to
    """
    input_path = pathlib.Path(filepath)
    output_path = pathlib.Path(output_dir)
    _recover_idx_file(input_path, output_path, verbose=True)


@app.command()
def recover_persistent_store(storepath: str, output_dir: str):
    """
    Command to extract all messages from a RabbitMQ persistent messages store

    :param storepath: path to the msg_store_persistent directory containing .rdq files
    :param output_dir: directory extracted messages will be written to
    """
    store_path = pathlib.Path(storepath)
    output_path = pathlib.Path(output_dir)
    _recover_persistent_store(store_path, output_path)


@app.command()
def recover_indexes(indexespath: str, output_dir: str):
    """
    Command to extract all unacked messages stored in a RabbitMQ indexes directory

    :param indexespath: path to the queues directory containing /id/.idx files
    :param output_dir: directory extracted messages will be written
    """
    indexes_path = pathlib.Path(indexespath)
    output_dir = pathlib.Path(output_dir)
    _recover_indexes(indexes_path, output_dir)


if __name__ == "__main__":
    app()
