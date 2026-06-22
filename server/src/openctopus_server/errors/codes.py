from enum import StrEnum


class ErrorCode(StrEnum):
    # Workspace
    WORKSPACE_NOT_FOUND = "workspace_not_found"
    WORKSPACE_PERMISSION_DENIED = "workspace_permission_denied"
    WORKSPACE_SYMLINK_ESCAPE = "workspace_symlink_escape"
    WORKSPACE_SOFT_LOCKED = "workspace_soft_locked"
    WORKSPACE_UPLOAD_TOO_LARGE = "workspace_upload_too_large"
    WORKSPACE_INVALID_SKILL_FORMAT = "workspace_invalid_skill_format"
    WORKSPACE_BLOCKED_PATH = "workspace_blocked_path"
    # Tool
    TOOL_AMBIGUOUS_EDIT = "tool_ambiguous_edit"
    TOOL_NO_MATCH = "tool_no_match"
    TOOL_IS_DIRECTORY = "tool_is_directory"
    TOOL_IS_FILE = "tool_is_file"
    TOOL_NOT_A_DIRECTORY = "tool_not_a_directory"
    TOOL_INVALID_NOTEBOOK = "tool_invalid_notebook"
    TOOL_CELL_INDEX_OUT_OF_RANGE = "tool_cell_index_out_of_range"
    TOOL_INVALID_ARGS = "tool_invalid_args"
    TOOL_INVALID_REGEX = "tool_invalid_regex"
    TOOL_INVALID_GLOB = "tool_invalid_glob"
    TOOL_EXEC_TIMEOUT = "tool_exec_timeout"
    TOOL_COMMAND_DENIED = "tool_command_denied"
    TOOL_ENV_NOT_ALLOWED = "tool_env_not_allowed"
    TOOL_CWD_OUTSIDE_WORKSPACE = "tool_cwd_outside_workspace"
    TOOL_PATH_OUTSIDE_WORKSPACE = "tool_path_outside_workspace"
    TOOL_DEVICE_UNREACHABLE = "tool_device_unreachable"
    TOOL_CHANNEL_NOT_CONFIGURED = "tool_channel_not_configured"
    TOOL_UNSUPPORTED_MEDIA = "tool_unsupported_media"
    TOOL_DELIVERY_FAILED = "tool_delivery_failed"
    TOOL_INVALID_SCHEDULE = "tool_invalid_schedule"
    TOOL_MISSING_REQUIRED_FIELD = "tool_missing_required_field"
    TOOL_DB_ERROR = "tool_db_error"
    TOOL_CRON_JOB_NOT_FOUND = "tool_cron_job_not_found"
    TOOL_MCP_UNAVAILABLE = "tool_mcp_unavailable"
    # Network
    NETWORK_SSRF_BLOCKED = "network_ssrf_blocked"
    NETWORK_DNS_FAILED = "network_dns_failed"
    NETWORK_TIMEOUT = "network_timeout"
    NETWORK_HTTP_ERROR = "network_http_error"
    # Protocol
    PROTOCOL_MALFORMED_FRAME = "protocol_malformed_frame"
    PROTOCOL_UNKNOWN_TYPE = "protocol_unknown_type"
    PROTOCOL_VERSION_MISMATCH = "protocol_version_mismatch"
    PROTOCOL_TRANSFER_UNKNOWN_ID = "protocol_transfer_unknown_id"
    # MCP
    MCP_WITHIN_SERVER_COLLISION = "mcp_within_server_collision"
    MCP_SCHEMA_COLLISION = "mcp_schema_collision"
    MCP_SPAWN_FAILED = "mcp_spawn_failed"
    # Auth
    AUTH_UNAUTHORIZED = "auth_unauthorized"
    AUTH_LAST_ADMIN_REQUIRED = "auth_last_admin_required"
    AUTH_EMAIL_TAKEN = "auth_email_taken"
    AUTH_INVALID_CREDENTIALS = "auth_invalid_credentials"
    AUTH_FORBIDDEN = "auth_forbidden"
    USER_NOT_FOUND = "user_not_found"
    # Config
    CONFIG_VALIDATION_FAILED = "config_validation_failed"
    # System
    SERVER_RESTART = "server_restart"
    USER_CANCELLED = "user_cancelled"
