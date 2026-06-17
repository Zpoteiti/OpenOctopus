from .codes import ErrorCode


class OpenOctopusError(Exception):
    def __init__(self, code: ErrorCode, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


class WorkspaceError(OpenOctopusError):
    pass


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
