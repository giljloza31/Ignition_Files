def handleMessage(payload):
	from shared.es_platform.commands.sudo import run_as_verified


	fn = payload.get("fn")
	kwargs = payload.get("kwargs") or {}
	session_user = payload.get("sessionUser")

	verify_user = payload.get("verifyUser")
	verify_pass = payload.get("verifyPass")

	systemCode = payload.get("systemCode")

	# Build your cmd + store here (however you're instantiating them)
	cmd = _get_command_helper(systemCode)

	# AD primary + Ignition fallback
	return run_as_verified(
		cmd,
		fn_name=fn,
		fn_kwargs=kwargs,
		verify_username=verify_user,
		verify_password=verify_pass,
		primary_source="MouserAD",
		fallback_source="Default",
		session_user=session_user
	)
	
	
	def _get_command_helper(systemCode):
		# TODO: replace with your real singleton/container getter
		from shared.foundation.mongo.proxy import MongoProxy
		from shared.es_platform.domain.state_store import StateStore
		from shared.es_platform.commands.command_helper import CommandHelper, CommandQueue
	
		mongo = MongoProxy(connector="MongoWCS", gateway_project="ES_Platform", handler_name="MongoProxy")
		store = StateStore(systemCode, mongo, site_tz_id="America/Chicago", enable_cache=True)
	
		queue = CommandQueue(max_size=300, min_ms_between=75, dedupe_window_ms=250)
	
		cmd = CommandHelper(
			systemCode,
			store,
			dry_run=False,
			use_queue=True,
			queue=queue
		)
	
		return cmd
	
# Expect payload dict:
# {
#   "fn": "system_off",
#   "kwargs": {},
#   "sessionUser": "joe",
#   "verifyUser": "supervisor1",
#   "verifyPass": "*****",
#   "systemCode": "MOUSER-ES-C1"
# }