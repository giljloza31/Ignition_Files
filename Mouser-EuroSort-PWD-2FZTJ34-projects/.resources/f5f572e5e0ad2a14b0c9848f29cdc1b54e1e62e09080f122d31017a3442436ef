# shared/es_platform/commands/receipt_view.py
# Helpers to convert receipt docs into a clean, UI-friendly table dataset (Perspective)

try:
	import system
except:
	system = None


DEFAULT_COLUMNS = [
	("createdAtLocal", "Time"),
	("status", "Status"),
	("eventType", "Command"),
	("chuteId", "Chute"),
	("carrierId", "Carrier"),
	("requestedBy", "RequestedBy"),
	("authorizedBy", "AuthorizedBy"),
	("durationMs", "DurationMs"),
	("error", "Error"),
	("_id", "CommandId"),
]


def to_rows(receipts, columns=None):
	"""
	Convert list[dict] receipts to list[dict] rows using friendly column names.
	Safe for Perspective table 'data' binding (array of objects).
	"""
	cols = columns or DEFAULT_COLUMNS
	out = []

	for r in (receipts or []):
		row = {}
		for key, label in cols:
			row[label] = _safe_get(r, key)
		out.append(row)

	return out


def to_dataset(receipts, columns=None):
	"""
	Convert receipts to an Ignition Dataset (for legacy tables or easy transforms).
	Returns system.dataset.toDataSet(headers, data) if system is available.
	Else returns {"headers":[...], "rows":[...]}.
	"""
	cols = columns or DEFAULT_COLUMNS
	headers = [label for (_k, label) in cols]

	data = []
	for r in (receipts or []):
		row = []
		for key, _label in cols:
			row.append(_safe_get(r, key))
		data.append(row)

	if system is None:
		return {"headers": headers, "rows": data}

	try:
		return system.dataset.toDataSet(headers, data)
	except:
		return {"headers": headers, "rows": data}


def to_perspective_value(receipts, columns=None):
	"""
	Best default for Perspective Table:
	- Returns list[dict] (array of objects)
	"""
	return to_rows(receipts, columns=columns)


def _safe_get(doc, key):
	if not isinstance(doc, dict):
		return None

	# Allow nested keys like "writeResult.0.quality" if you ever want later
	if "." in str(key):
		return _safe_get_nested(doc, str(key))

	# normal
	if key in doc:
		return doc.get(key)

	# A few common fallbacks
	if key == "createdAtLocal":
		return doc.get("createdAtLocal") or doc.get("createdAtUtc")
	if key == "authorizedBy":
		return doc.get("authorizedBy") or doc.get("authorizedUser") or doc.get("authUser")

	return None


def _safe_get_nested(doc, dotted):
	parts = dotted.split(".")
	cur = doc
	for p in parts:
		if isinstance(cur, dict):
			cur = cur.get(p)
		elif isinstance(cur, (list, tuple)):
			try:
				cur = cur[int(p)]
			except:
				return None
		else:
			return None
	return cur
	

#
#from shared.foundation.mongo.proxy import MongoProxy
#from shared.es_platform.commands.receipt_api import ReceiptAPI
#from shared.es_platform.commands.receipt_view import to_perspective_value
#
#mongo = MongoProxy(connector="MongoWCS", gateway_project="ES_Platform", handler_name="MongoProxy")
#api = ReceiptAPI(mongo, systemCode="MOUSER-ES-C1")
#
#receipts = api.recent(limit=50)
#table_data = to_perspective_value(receipts)   # list of dicts for Perspective Table
#
#return table_data

#
#from shared.es_platform.commands.receipt_view import to_perspective_value
#
#OPERATOR_COLUMNS = [
#	("createdAtLocal", "Time"),
#	("status", "Status"),
#	("eventType", "Command"),
#	("chuteId", "Chute"),
#	("requestedBy", "User"),
#	("authorizedBy", "Auth"),
#	("error", "Error"),
#]
#
#table_data = to_perspective_value(receipts, columns=OPERATOR_COLUMNS)