"""Small, deliberately non-screen terminal normalizer for exec PTY output."""

from __future__ import annotations

import codecs
from dataclasses import dataclass


@dataclass
class TerminalNormalizer:
    """Normalize PTY bytes into line-oriented text.

    This is not a terminal emulator.  It strips ANSI CSI/OSC controls, maps both
    CR forms to LF, and only applies backspace to text accumulated in the current
    call.  DSR replies are exposed for the PTY adapter to write directly.
    """

    escape_limit: int = 256

    def __post_init__(self) -> None:
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self._escape = bytearray()
        self._mode = "text"
        self._saw_cr = False
        self._discard_saw_escape = False
        self._responses: list[bytes] = []
        self.control_truncated = False

    def feed(self, data: bytes) -> str:
        fragment: list[str] = []
        index = 0
        while index < len(data):
            byte = data[index]
            index += 1
            if self._mode == "text":
                if self._saw_cr:
                    self._saw_cr = False
                    if byte == 0x0A:
                        continue
                if byte == 0x1B:
                    self._flush_pending(fragment)
                    self._mode = "escape"
                    self._escape = bytearray([byte])
                    continue
                self._consume_text_byte(byte, fragment)
                continue
            if self._mode == "discard_csi":
                if 0x40 <= byte <= 0x7E:
                    self._mode = "text"
                continue
            if self._mode == "discard_osc":
                if byte == 0x07 or (self._discard_saw_escape and byte == ord("\\")):
                    self._mode = "text"
                    self._discard_saw_escape = False
                else:
                    self._discard_saw_escape = byte == 0x1B
                continue
            self._escape.append(byte)
            if len(self._escape) > self.escape_limit:
                self._mode = "discard_osc" if self._mode == "osc" else "discard_csi"
                self._escape.clear()
                self.control_truncated = True
                continue
            if self._mode == "escape":
                if byte == ord("["):
                    self._mode = "csi"
                elif byte == ord("]"):
                    self._mode = "osc"
                else:
                    self._mode = "text"
                    self._escape.clear()
                continue
            if self._mode == "csi":
                if 0x40 <= byte <= 0x7E:
                    self._finish_csi(bytes(self._escape))
                    self._mode = "text"
                    self._escape.clear()
                continue
            # OSC terminates with BEL or ST (ESC backslash).
            if byte == 0x07:
                self._mode = "text"
                self._escape.clear()
            elif len(self._escape) >= 2 and self._escape[-2:] == b"\x1b\\":
                self._mode = "text"
                self._escape.clear()
        return "".join(fragment)

    def flush(self) -> str:
        """Flush an incomplete UTF-8 scalar at end-of-stream."""

        return self._flush_decoder()

    @property
    def pending_control_bytes(self) -> int:
        return len(self._escape)

    @property
    def pending_response_count(self) -> int:
        return len(self._responses)

    @property
    def responses(self) -> list[bytes]:
        """Compatibility snapshot; adapters must drain with ``take_responses``."""

        return list(self._responses)

    def take_responses(self) -> tuple[bytes, ...]:
        responses = tuple(self._responses)
        self._responses.clear()
        return responses

    def _consume_text_byte(self, byte: int, output: list[str]) -> None:
        if byte in {0x0A, 0x0D}:
            self._flush_pending(output)
            output.append("\n")
            if byte == 0x0D:
                self._saw_cr = True
            return
        if byte == 0x08:
            self._flush_pending(output)
            if output and output[-1] != "\n":
                output.pop()
            return
        if byte == 0x09:
            self._flush_pending(output)
            output.append("\t")
            return
        if byte < 0x20 or byte == 0x7F:
            self._flush_pending(output)
            return
        decoded = self._decoder.decode(bytes((byte,)), final=False)
        output.extend(decoded)

    def _flush_pending(self, output: list[str]) -> None:
        output.extend(self._flush_decoder())

    def _flush_decoder(self) -> str:
        text = self._decoder.decode(b"", final=True)
        self._decoder.reset()
        return text

    def _finish_csi(self, sequence: bytes) -> None:
        if sequence == b"\x1b[5n":
            self._responses.append(b"\x1b[0n")
        elif sequence == b"\x1b[6n":
            self._responses.append(b"\x1b[1;1R")
        elif sequence == b"\x1b[?6n":
            self._responses.append(b"\x1b[?1;1R")
