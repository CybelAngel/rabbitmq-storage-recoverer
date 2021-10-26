"""
Interact with RabbitMQ index files
"""

import pathlib
from collections import defaultdict
from typing import List, Tuple, Dict
from term import codec
import typer
from model import Message, message_from_decoded_etf

RECORD_TYPE_MASK = 0b1100000000000000
RECORD_SEQ_ID_MASK = 0b0011111111111111
STATUS_MAPPING = {0: "published", 1: "delivered", 2: "acknowledged"}


def parse_idx_file(filepath: pathlib.Path) -> List[Message]:
    """
    Parse an index file and return stored messages that have not been acked.
    :param filepath: path to the .idx file
    :return: a list containing all messages that were stored in the index
    """

    file_length = filepath.stat().st_size

    messages = dict()  # sequence id to Message
    # We use a default dict to count the number of AckOrDeliver records concerning a message.
    # A message always has a publish record, and then possibly one or two AckOrDeliver records.
    # So all messages should have a status of O (published), 1 (delivered) or 2 (acknowledged).
    messages_status_by_seq_id = defaultdict(int)

    with filepath.open("rb") as input_file:
        while input_file.tell() < file_length:
            # Each record is at least two bytes long. The first two bits define the record type: 01 for del/ack and 10 or 11 for publish.
            record_start = int.from_bytes(input_file.read(2), byteorder="big")
            record_type = (record_start & RECORD_TYPE_MASK) >> 14
            record_seq_id = record_start & RECORD_SEQ_ID_MASK

            if record_type == 0b01:  # Acknowledge or deliver record
                messages_status_by_seq_id[record_seq_id] += 1
            elif record_type == 0b11:  # Publish record for persistent message
                message_id = int.from_bytes(input_file.read(16), byteorder="big")
                message_expiry = int.from_bytes(input_file.read(8), byteorder="big")
                assert message_expiry == 0
                # Skip size since we don't need it
                _ = int.from_bytes(input_file.read(4), byteorder="big")
                message_embedded_size = int.from_bytes(input_file.read(4), byteorder="big")

                if message_embedded_size != 0:
                    # Message is stored directly in the index
                    message = input_file.read(message_embedded_size)

                    # It is serialized in Erlang External Term Format
                    # Complete reference: https://erlang.org/doc/apps/erts/erl_ext_dist.html
                    try:
                        decoded_message = codec.binary_to_term(message)
                    except:
                        # If the file corrupted, the codec won't be able to decode the message
                        # We don't have a write ok marker like in the store
                        typer.secho(
                            f"Invalid ETF data found. Aborting recovery for this file.",
                            fg=typer.colors.RED,
                        )
                        continue

                    structured_message = message_from_decoded_etf(message_id, decoded_message)
                    messages[record_seq_id] = structured_message
                else:
                    # Message is stored in the store
                    continue

    unacked_messages = []  # This will contain all unacked messages

    # Browse the dict of all messages that are stored in the index, prune all those who have been acknowledged
    for seq_id, message in messages.items():
        message_status = messages_status_by_seq_id[seq_id]
        if message_status == 0 or message_status == 1:
            # Message has been published or delivered, but not acknowledged
            unacked_messages.append(message)

    return unacked_messages
