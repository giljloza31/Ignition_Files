"""
foundation.time.clock

ES_2.0 time utilities.

Contract:
	- Always keep an absolute timestamp (java.util.Date / epoch millis)
	- Always be able to format local time per SITE timezone
	- Store timezone ID used (tzId)
	- Provide safe time difference helpers

Local display format:
	yyyyMMdd HH:mm:ss.SSS
"""

from java.text import SimpleDateFormat
from java.util import Date, TimeZone


# ----------------------------
# Internal helpers
# ----------------------------

def _get_formatter(pattern, tz_id):
	tz = TimeZone.getTimeZone(tz_id)
	fmt = SimpleDateFormat(pattern)
	fmt.setTimeZone(tz)
	return fmt


def _to_millis(val):
	"""
	Normalize input to epoch milliseconds.
	Accepts:
		- java.util.Date
		- UTC ISO string yyyy-MM-dd'T'HH:mm:ss.SSSZ
	"""
	if val is None:
		return None

	if hasattr(val, "getTime"):
		return val.getTime()

	if isinstance(val, basestring):
		d = safe_parse_utc_iso(val)
		if d:
			return d.getTime()

	return None


# ----------------------------
# Now helpers
# ----------------------------

def now_date():
	"""Return current time as java.util.Date."""
	return Date()


def now_utc_iso():
	"""Return current time as ISO-8601 UTC string."""
	fmt = _get_formatter("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "UTC")
	return fmt.format(Date())


def now_local_string(tz_id, pattern="yyyyMMdd HH:mm:ss.SSS"):
	"""Return current time formatted in the given timezone."""
	fmt = _get_formatter(pattern, tz_id)
	return fmt.format(Date())


# ----------------------------
# Formatting helpers
# ----------------------------

def to_utc_iso(date_obj):
	"""Format java.util.Date to ISO-8601 UTC string."""
	if date_obj is None:
		return None
	fmt = _get_formatter("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "UTC")
	return fmt.format(date_obj)


def to_local_string(date_obj, tz_id, pattern="yyyyMMdd HH:mm:ss.SSS"):
	"""Format java.util.Date to local timezone string."""
	if date_obj is None:
		return None
	fmt = _get_formatter(pattern, tz_id)
	return fmt.format(date_obj)


# ----------------------------
# Timestamp packer (USE THIS)
# ----------------------------

def pack_timestamps(date_obj=None, tz_id="UTC"):
	"""
	Return a complete timestamp bundle for state/audit writes.

	Returns:
	{
		"tsDate":   java.util.Date,
		"tsEpoch": int (milliseconds),
		"tsUtc":   ISO-8601 UTC string,
		"tsLocal": local formatted string,
		"tzId":    timezone ID used
	}
	"""
	d = date_obj or Date()
	fmt_utc = _get_formatter("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "UTC")
	fmt_local = _get_formatter("yyyyMMdd HH:mm:ss.SSS", tz_id)

	return {
		"tsDate": d,
		"tsEpoch": d.getTime(),
		"tsUtc": fmt_utc.format(d),
		"tsLocal": fmt_local.format(d),
		"tzId": tz_id,
	}


# ----------------------------
# Parsing helpers
# ----------------------------

def safe_parse_utc_iso(iso_str):
	"""
	Parse ISO-8601 UTC string produced by this module.
	Returns java.util.Date or None.
	"""
	if not iso_str:
		return None

	fmt = _get_formatter("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "UTC")
	try:
		return fmt.parse(iso_str)
	except:
		return None


# ----------------------------
# Time difference helpers (LOCKED)
# ----------------------------

def diff_ms(a, b, absolute=True):
	"""
	Time difference in milliseconds.
	Accepts Date or UTC ISO string.
	"""
	am = _to_millis(a)
	bm = _to_millis(b)
	if am is None or bm is None:
		return None

	d = bm - am
	return abs(d) if absolute else d


def diff_seconds(a, b, absolute=True, precision=3):
	"""Time difference in seconds."""
	ms = diff_ms(a, b, absolute)
	if ms is None:
		return None
	return round(ms / 1000.0, precision)


def diff_minutes(a, b, absolute=True, precision=3):
	"""Time difference in minutes."""
	ms = diff_ms(a, b, absolute)
	if ms is None:
		return None
	return round(ms / 60000.0, precision)


def diff_hours(a, b, absolute=True, precision=3):
	"""Time difference in hours."""
	ms = diff_ms(a, b, absolute)
	if ms is None:
		return None
	return round(ms / 3600000.0, precision)


def diff(a, b):
	"""
	Return all common deltas as a dict.
	"""
	ms = diff_ms(a, b, absolute=True)
	if ms is None:
		return None

	return {
		"ms": ms,
		"seconds": round(ms / 1000.0, 3),
		"minutes": round(ms / 60000.0, 3),
		"hours": round(ms / 3600000.0, 3),
	}