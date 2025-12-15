
"""
foundation.mongo.proxy

MongoProxy for ES_Platform.

Works in multiple scopes by auto-selecting backend:
	1) Direct: system.mongodb.* (if available in current scope)
	2) Gateway message handler via system.util.sendRequest (Designer/Client safe)

Requires (if using #2):
	Project Message Handler: ES_Platform / MongoProxy
"""

import system


class MongoProxy(object):
	def __init__(self,
			connector,
			gateway_project="ES_Platform",
			handler_name="MongoProxy",
			timeout_ms=10000):
		if not connector:
			raise ValueError("MongoProxy requires a Mongo connector name")
		self.connector = connector
		self.gateway_project = gateway_project
		self.handler_name = handler_name
		self.timeout_ms = int(timeout_ms)

	def _has_system_mongodb(self):
		m = getattr(system, "mongodb", None)
		return m is not None

	def _call_direct(self, fn_name, collection, *args, **kwargs):
		mongodb = getattr(system, "mongodb", None)
		if not mongodb:
			raise RuntimeError("system.mongodb is not available in this scope")

		fn = getattr(mongodb, fn_name, None)
		if not fn:
			raise AttributeError("system.mongodb.%s not found" % fn_name)

		return fn(self.connector, collection, *args, **kwargs)

	def _call_gateway(self, fn_name, collection, *args, **kwargs):
		payload = {
			"fn": fn_name,
			"connector": self.connector,
			"collection": collection,
			"args": list(args),
			"kwargs": dict(kwargs),
		}

		resp = system.util.sendRequest(
			project=self.gateway_project,
			messageHandler=self.handler_name,
			payload=payload,
			timeout=self.timeout_ms
		)

		if not isinstance(resp, dict):
			raise RuntimeError("MongoProxy gateway response was not a dict: %r" % resp)

		if not resp.get("ok"):
			raise RuntimeError("MongoProxy gateway error: %s" % resp.get("error"))

		return resp.get("result")

	def _call(self, fn_name, collection, *args, **kwargs):
		# Try direct first
		if self._has_system_mongodb():
			return self._call_direct(fn_name, collection, *args, **kwargs)

		# Fallback to gateway message handler
		return self._call_gateway(fn_name, collection, *args, **kwargs)

	# ----------------------------
	# Public API
	# ----------------------------

	def find_one(self, collection, filt=None, **opts):
		return self._call("findOne", collection, filt or {}, **opts)

	def find(self, collection, filt=None, **opts):
		return self._call("find", collection, filt or {}, **opts)

	def insert_one(self, collection, doc, **opts):
		return self._call("insertOne", collection, doc or {}, **opts)

	def insert_many(self, collection, docs, **opts):
		return self._call("insertMany", collection, docs or [], **opts)

	def update_one(self, collection, filt, update_doc, upsert=False, **opts):
		opts.setdefault("upsert", bool(upsert))
		return self._call("updateOne", collection, filt or {}, update_doc or {}, **opts)

	def update_many(self, collection, filt, update_doc, upsert=False, **opts):
		opts.setdefault("upsert", bool(upsert))
		return self._call("updateMany", collection, filt or {}, update_doc or {}, **opts)

	def replace_one(self, collection, filt, doc, upsert=False, **opts):
		opts.setdefault("upsert", bool(upsert))
		return self._call("replaceOne", collection, filt or {}, doc or {}, **opts)

	def delete_one(self, collection, filt, **opts):
		return self._call("deleteOne", collection, filt or {}, **opts)

	def delete_many(self, collection, filt, **opts):
		return self._call("deleteMany", collection, filt or {}, **opts)

	def upsert_one(self, collection, key, fields, **opts):
		update_doc = {"$set": dict(fields or {})}
		opts.setdefault("upsert", True)
		return self._call("updateOne", collection, key or {}, update_doc, **opts)