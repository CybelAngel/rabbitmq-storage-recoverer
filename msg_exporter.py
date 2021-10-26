"""
Export parsed messages into a clean hierarchy
"""

import pathlib
from model import Message


def dump_message(message: Message, output_dir: pathlib.Path):
    """Dump a parsed message on the disk. The path will look like this: output_dir/exchange/routing_key (if applicable)/message"""

    # Compute the path at which the message will be stored
    base_path = output_dir / message.exchange
    if message.routing_key:
        base_path = base_path / message.routing_key

    # Create the filesystem tree if needed
    base_path.mkdir(mode=0o755, parents=True, exist_ok=True)

    # Write the file to disk
    file_path = base_path / str(message.id)
    with file_path.open("w") as output_file:
        output_file.write(message.body)


def delete_message(message_id: int, output_dir: pathlib.Path):
    """Delete a message that has been dumped to disk, for example when we know it has been acknowledged"""
    pass
