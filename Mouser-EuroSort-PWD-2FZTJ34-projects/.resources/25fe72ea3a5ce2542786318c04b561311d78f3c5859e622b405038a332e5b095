# shared/es_platform/commands/receipt_format.py
# Formatting + UI helpers for receipts (Perspective-friendly)

DEFAULT_STATUS_COLORS = {
	"ACK": "#2e7d32",		# green
	"SENT": "#1565c0",		# blue
	"QUEUED": "#6d4c41",	# brown
	"FAILED": "#c62828",	# red
	"TIMEOUT": "#ef6c00",	# orange
	"CANCELED": "#616161",	# gray
}

DEFAULT_STATUS_ICONS = {
	"ACK": "check_circle",
	"SENT": "send",
	"QUEUED": "schedule",
	"FAILED": "error",
	"TIMEOUT": "timer_off",
	"CANCELED": "cancel",
}


def format_duration_ms(ms, decimals=3):
	"""
	1532 -> "1.532s"
	80   -> "80ms"
	0/None -> ""
	"""
	if ms is None:
		return ""
	try:
		msi = int(ms)
	except:
		return ""

	if msi < 0:
		msi = 0

	if msi < 1000:
		return "%dms" % msi

	sec = msi / 1000.0
	fmt = "%%.%dfs" % int(decimals)
	return fmt % sec


def short_command(eventType):
	"""
	Make command names operator-friendly.
	"""
	if not eventType:
		return ""
	s = str(eventType)

	m = {
		"CMD_SYSTEM_ON": "System On",
		"CMD_SYSTEM_OFF": "System Off",
		"CMD_SET_MODE": "Set Mode",
		"CMD_CHUTE_OPEN": "Open Door",
		"CMD_CHUTE_CLOSE": "Close Door",
		"CMD_CHUTE_LIGHT": "Light",
		"CMD_CARRIER_FORCE_RELEASE": "Force Release",
	}
	if s in m:
		return m[s]

	# Generic: strip CMD_ and title-case
	if s.startswith("CMD_"):
		s = s[4:]
	return s.replace("_", " ").title()


def status_color(status, mapping=None):
	"""
	Return hex color for status.
	"""
	if not status:
		return "#000000"
	m = mapping or DEFAULT_STATUS_COLORS
	return m.get(str(status), "#000000")


def status_icon(status, mapping=None):
	"""
	Return a material icon name (for Perspective Icon component).
	"""
	if not status:
		return "help"
	m = mapping or DEFAULT_STATUS_ICONS
	return m.get(str(status), "help")


def row_style_for_status(status):
	"""
	Simple style object for Perspective Table row style transform.
	"""
	return {
		"color": status_color(status),
		"fontWeight": "600" if str(status) in ("FAILED", "TIMEOUT") else "400"
	}


def enrich_receipt_row(row):
	"""
	Given a UI row dict (like from receipt_view.to_rows),
	add display fields:
	- Duration (pretty)
	- Command (pretty)
	- StatusColor
	- StatusIcon
	"""
	if not isinstance(row, dict):
		return row

	# tolerate either label names or raw keys
	status = row.get("Status") or row.get("status")
	cmd = row.get("Command") or row.get("eventType")
	dur = row.get("DurationMs") or row.get("durationMs")

	row["Duration"] = format_duration_ms(dur)
	row["CommandPretty"] = short_command(cmd)
	row["StatusColor"] = status_color(status)
	row["StatusIcon"] = status_icon(status)

	return row


def enrich_rows(rows):
	"""
	Apply enrich_receipt_row across list of rows.
	"""
	out = []
	for r in (rows or []):
		try:
			out.append(enrich_receipt_row(r))
		except:
			out.append(r)
	return out
	
	

#
#from shared.es_platform.commands.receipt_api import ReceiptAPI
#from shared.es_platform.commands.receipt_view import to_rows
#from shared.es_platform.commands.receipt_format import enrich_rows
#from shared.foundation.mongo.proxy import MongoProxy
#
#mongo = MongoProxy(connector="MongoWCS", gateway_project="ES_Platform", handler_name="MongoProxy")
#api = ReceiptAPI(mongo, systemCode="MOUSER-ES-C1")
#
#receipts = api.recent(limit=50)
#rows = to_rows(receipts)
#rows = enrich_rows(rows)
#
#return rows