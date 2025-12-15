# shared/es_platform/commands/__init__.py
"""
ES_Platform Command Layer

Provides:
- CommandHelper: high-level operator & automation commands
- CommandQueue: throttling, dedupe, and async execution
- Permissions: role-based authorization with re-auth support
- Receipts: persistent command lifecycle tracking
- Tag mapping helpers

Designed for:
- Ignition Gateway + Perspective
- UI-safe permission errors
- Auditable, replayable command flows
"""

# Core helpers
from shared.es_platform.commands.command_helper import CommandHelper
from shared.es_platform.commands.queue import CommandQueue

# Permissions
from shared.es_platform.commands.permissions import (
	CommandAuthorizer,
	PermissionDenied,
	default_rules,
)

# Receipts
from shared.es_platform.commands.receipt_store import ReceiptStore

# Tag helpers
from shared.es_platform.commands import tagmap