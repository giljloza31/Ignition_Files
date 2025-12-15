try:
	import system
except:
	system = None


class AuthResult(object):
	def __init__(self, ok, username=None, roles=None, source=None, reason=None):
		self.ok = bool(ok)
		self.username = username
		self.roles = roles or []
		self.source = source
		self.reason = reason


def verify_credentials(username, password, primary_source=None, fallback_source=None):
	"""
	Verify username/password against:
		1) primary_source (Active Directory user source)
		2) fallback_source (Ignition user source)

	Does NOT change the currently logged-in user (Joe stays logged in).
	"""
	if system is None:
		return AuthResult(False, username=username, roles=[], source=None, reason="system_not_available")

	u = (username or "").strip()
	p = password or ""
	if not u or not p:
		return AuthResult(False, username=u, roles=[], source=None, reason="missing_username_or_password")

	# Build ordered list of sources to try
	sources = []
	if primary_source:
		sources.append(str(primary_source))
	if fallback_source and str(fallback_source) not in sources:
		sources.append(str(fallback_source))

	# If none provided, we still try the simple authenticate(u,p) as a last resort.
	if not sources:
		try:
			ok = bool(system.security.authenticate(u, p))
			if not ok:
				return AuthResult(False, username=u, roles=[], source=None, reason="invalid_credentials")
			roles = _get_roles_best_effort(None, u)
			return AuthResult(True, username=u, roles=roles, source=None, reason=None)
		except Exception as e:
			return AuthResult(False, username=u, roles=[], source=None, reason="auth_exception:%s" % e)

	last_err = None

	for src in sources:
		try:
			ok = bool(system.security.authenticate(src, u, p))
			if not ok:
				last_err = "invalid_credentials"
				continue

			roles = _get_roles_best_effort(src, u)

			return AuthResult(True, username=u, roles=roles, source=src, reason=None)

		except Exception as e:
			last_err = "auth_exception:%s" % e

	return AuthResult(False, username=u, roles=[], source=None, reason=last_err or "invalid_credentials")


def _get_roles_best_effort(user_source, username):
	"""
	Roles lookup across scopes.
	Prefer system.user.getRoles(userSource, username) when available.
	Fallback to system.security.getRoles(username).
	"""
	roles = []

	if system is None:
		return roles

	# Best: system.user.getRoles(userSource, username)
	try:
		if user_source:
			roles = list(system.user.getRoles(str(user_source), str(username)))
			return roles
	except:
		pass

	# Fallback: system.security.getRoles(username) (scope-dependent)
	try:
		roles = list(system.security.getRoles(str(username)))
		return roles
	except:
		pass

	return roles