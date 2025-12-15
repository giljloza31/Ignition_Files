# shared/es_platform/commands/sudo.py
# Privileged runner: verify supervisor creds WITHOUT changing session user.

from shared.es_platform.commands.permissions import build_auth_context, PermissionDenied


def run_as_verified(cmd, fn_name, fn_kwargs,
		verify_username, verify_password,
		primary_source=None, fallback_source=None,
		session_user=None, site_tz_id="UTC", logger=None):
	"""
	- cmd: CommandHelper instance
	- fn_name: name of CommandHelper method (string)
	- fn_kwargs: dict of kwargs to pass to the command (ex: {"dst": "...", "on": True})
	- verify_username/password: supervisor/admin creds for re-auth
	- session_user: the currently logged-in operator (Joe)
	"""

	# Convert your "primary/fallback" into the permissions module "user_sources" list
	user_sources = []
	if primary_source:
		user_sources.append(primary_source)
	if fallback_source and fallback_source not in user_sources:
		user_sources.append(fallback_source)

	# Default matches what you said: Active Directory + Ignition fallback
	if not user_sources:
		user_sources = ["Active Directory", "Ignition"]

	try:
		ctx = build_auth_context(
			username=verify_username,
			password=verify_password,
			user_sources=user_sources,
			site_tz_id=site_tz_id,
			logger=logger
		)
	except PermissionDenied as e:
		# UI-safe denial payload
		return {
			"ok": False,
			"authorized": False,
			"reason": "auth_failed",
			"message": str(e),
			"payload": getattr(e, "payload", {}) or {}
		}
	except Exception as e:
		# Unexpected error (still keep it UI-safe)
		return {
			"ok": False,
			"authorized": False,
			"reason": "auth_error",
			"message": str(e)
		}

	# Build context for CommandHelper (same shape you were already passing)
	# ctx already includes: authUser, authSource, roles, ts
	fn = getattr(cmd, fn_name, None)
	if fn is None:
		return {"ok": False, "authorized": True, "reason": "unknown_command:%s" % fn_name}

	kwargs = dict(fn_kwargs or {})
	kwargs.setdefault("userId", session_user)  # Joe (session user)
	kwargs.setdefault("context", ctx)          # Admin/Supervisor creds

	res = fn(**kwargs)

	return {
		"ok": True,
		"authorized": True,
		"session_user": session_user,
		"auth_user": ctx.get("authUser"),
		"auth_source": ctx.get("authSource"),
		"auth_roles": ctx.get("roles"),
		"result": res
	}