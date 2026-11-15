import socket
import ssl
from collections.abc import Iterator

HOST = "irc.chat.twitch.tv"
PORT = 6697
ANONYMOUS = "justinfan31337"
READ_SECONDS = 5.0
BUFFER = 4096
PRIVMSG = " PRIVMSG #"
PING = "PING"
PONG = b"PONG :tmi.twitch.tv\r\n"


def connected(channel: str) -> ssl.SSLSocket:
    raw = socket.create_connection((HOST, PORT), timeout=READ_SECONDS)
    secured = ssl.create_default_context().wrap_socket(raw, server_hostname=HOST)
    secured.sendall(f"NICK {ANONYMOUS}\r\nJOIN #{channel}\r\n".encode())
    return secured


def lines(connection: ssl.SSLSocket) -> Iterator[str | None]:
    pending = b""
    while True:
        try:
            received = connection.recv(BUFFER)
        except TimeoutError:
            yield None
            continue
        if not received:
            raise ConnectionError("Twitch closed the chat connection")
        pending += received
        *complete, pending = pending.split(b"\r\n")
        for line in complete:
            yield line.decode("utf-8", errors="replace")


def sender(line: str) -> str | None:
    if PRIVMSG not in line or not line.startswith(":"):
        return None
    return line[1:].split("!", 1)[0]
