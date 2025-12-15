def handleMessage(payload):
	# Message Handler: MongoProxy
	# Project: ES_Platform
	# Name: MongoProxy
	

	fn = payload.get("fn")
	connector = payload.get("connector")
	collection = payload.get("collection")
	args = payload.get("args") or []
	kwargs = payload.get("kwargs") or {}

	mongodb = getattr(system, "mongodb", None)
	if not mongodb:
		return {"ok": False, "error": "system.mongodb not available in Gateway scope"}

	method = getattr(mongodb, fn, None)
	if not method:
		return {"ok": False, "error": "system.mongodb.%s not found" % fn}

	try:
		result = method(connector, collection, *args, **kwargs)
		return {"ok": True, "result": result}
	except Exception as e:
		return {"ok": False, "error": str(e)}