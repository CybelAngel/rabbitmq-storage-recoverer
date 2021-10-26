import zlib
from collections import namedtuple
import typer

Message = namedtuple("Message", ["id", "vhost", "exchange", "routing_key", "body"])


def message_from_decoded_etf(message_id: int, decoded_message):
    # Message structure is a bit complicated, so trust me on this one. Pprint the decoded_message if needed!
    # pprint(decoded_message)
    assert decoded_message[0][0] == "basic_message"
    assert decoded_message[0][1][0] == "resource"
    message_vhost = decoded_message[0][1][1].decode("utf-8")
    assert decoded_message[0][1][2] == "exchange"
    message_exchange = decoded_message[0][1][3].decode("utf-8")
    assert len(decoded_message[0][2]) == 1
    message_routing_key = decoded_message[0][2][0].decode("utf-8")

    # The body itself can be in multiple chunks if its very long. They need to be reassembled in reverse order, for some reason
    message_body_parts = decoded_message[0][3][5]
    reassembled_body = b""
    for body_chunk in message_body_parts[::-1]:
        reassembled_body += body_chunk

    try:
        message_body = reassembled_body.decode("utf-8")
    except:
        typer.secho(
            f"Found a message with id {message_id} for {message_vhost}{message_exchange}:{message_routing_key} that doesn't look like utf-8. Trying to decompress it.",
            fg=typer.colors.YELLOW,
        )
        decompressed_body = zlib.decompress(reassembled_body)
        message_body = decompressed_body.decode("utf-8")
        typer.secho(f"Message was zlib compressed!", fg=typer.colors.GREEN)

    return Message(
        id=message_id,
        vhost=message_vhost,
        exchange=message_exchange,
        routing_key=message_routing_key if message_routing_key else None,
        body=message_body,
    )
