"""
foundation.mongo.indexes

ES_Platform index PLAN (definition only).
We do NOT auto-create indexes (production-friendly).

Usage:
	from shared.foundation.mongo import indexes
	plan = indexes.get_index_plan()
	print(indexes.pretty_print_plan(plan))
	print(indexes.to_mongo_shell(plan, db_name="YOUR_DB_NAME"))
"""

import json


def get_index_plan():
	"""
	Returns a dict:
		collection -> list of index specs

	Index spec format:
	{
		"name": "index_name",
		"keys": [("field", 1), ("other", -1)],
		"unique": True/False,
		"partialFilterExpression": {...} (optional)
	}
	"""
	return {
		"es_platform_state": [
			{
				"name": "uq_state_entity",
				"keys": [("systemCode", 1), ("entityType", 1), ("entityId", 1)],
				"unique": True
			},
			{
				"name": "idx_state_updated",
				"keys": [("systemCode", 1), ("updatedAtEpoch", -1)],
				"unique": False
			},
			{
				"name": "idx_state_chute_status",
				"keys": [("systemCode", 1), ("entityType", 1), ("status", 1)],
				"unique": False,
				"partialFilterExpression": {"entityType": "CHUTE"}
			},
			{
				"name": "idx_state_carrier_phase",
				"keys": [("systemCode", 1), ("entityType", 1), ("currentPhase", 1)],
				"unique": False,
				"partialFilterExpression": {"entityType": "CARRIER"}
			},
		],

		"es_platform_events": [
			{
				"name": "idx_events_time",
				"keys": [("systemCode", 1), ("tsEpoch", -1)],
				"unique": False
			},
			{
				"name": "idx_events_corr",
				"keys": [("systemCode", 1), ("corrId", 1), ("tsEpoch", -1)],
				"unique": False
			},
			{
				"name": "idx_events_type",
				"keys": [("systemCode", 1), ("eventType", 1), ("tsEpoch", -1)],
				"unique": False
			},
		],

		"es_platform_layout_perfectpick": [
			{
				"name": "uq_layout_lane",
				"keys": [("systemCode", 1), ("ppLane", 1)],
				"unique": True
			},
			{
				"name": "idx_layout_consolZoneKey",
				"keys": [("systemCode", 1), ("consolZoneKey", 1)],
				"unique": False
			},
		],

		"es_platform_outbox_mouser": [
			{
				"name": "idx_outbox_status",
				"keys": [("systemCode", 1), ("status", 1), ("nextAttemptEpoch", 1)],
				"unique": False
			},
			{
				"name": "uq_outbox_idempotency",
				"keys": [("systemCode", 1), ("idempotencyKey", 1)],
				"unique": True
			},
		],
	}


def pretty_print_plan(plan=None):
	"""
	Returns a human-readable multiline string of the plan.
	"""
	p = plan or get_index_plan()
	lines = []
	for coll in sorted(p.keys()):
		lines.append("Collection: %s" % coll)
		for spec in p[coll]:
			keys_str = ", ".join(["%s:%s" % (k, v) for (k, v) in spec.get("keys", [])])
			lines.append("	- %s  keys=[%s]  unique=%s" % (
				spec.get("name"),
				keys_str,
				spec.get("unique", False)
			))
			pfe = spec.get("partialFilterExpression")
			if pfe:
				lines.append("		partialFilterExpression=%s" % json.dumps(pfe, sort_keys=True))
		lines.append("")
	return "\n".join(lines)


def to_mongo_shell(plan=None, db_name="DB_NAME"):
	"""
	Generate Mongo shell commands for creating indexes.
	Copy/paste into mongosh.

	Note: this is helper output only; we are not executing anything from Ignition.
	"""
	p = plan or get_index_plan()
	lines = []
	lines.append("use %s" % db_name)
	lines.append("")

	for coll in sorted(p.keys()):
		for spec in p[coll]:
			keys_obj = _keys_to_shell_obj(spec.get("keys", []))
			opts = {}
			if spec.get("name"):
				opts["name"] = spec.get("name")
			if spec.get("unique"):
				opts["unique"] = True
			if spec.get("partialFilterExpression"):
				opts["partialFilterExpression"] = spec.get("partialFilterExpression")

			lines.append("db.%s.createIndex(%s, %s)" % (
				coll,
				keys_obj,
				json.dumps(opts, sort_keys=True)
			))
		lines.append("")

	return "\n".join(lines)


def _keys_to_shell_obj(keys):
	"""
	Convert [("a",1),("b",-1)] to a Mongo shell object string: {a:1,b:-1}
	"""
	parts = []
	for k, v in keys:
		parts.append("%s:%s" % (k, v))
	return "{%s}" % ",".join(parts)