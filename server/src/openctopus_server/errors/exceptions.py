from collections.abc import Mapping

from .codes import ErrorCode


class OpenOctopusError(Exception):
    def __init__(self, code: ErrorCode, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


class WorkspaceError(OpenOctopusError):
    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(code, message)
        self.headers = dict(headers) if headers is not None else None


class ToolError(OpenOctopusError):
    pass


class NetworkError(OpenOctopusError):
    pass


class ProtocolError(OpenOctopusError):
    pass


class McpError(OpenOctopusError):
    pass


class AuthError(OpenOctopusError):
    pass


class ConfigError(OpenOctopusError):
    pass


class ChatError(OpenOctopusError):
    pass
