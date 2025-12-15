# shared/es_platform/domain/events.py

from shared.foundation.time import clock


COL_EVENTS = "es_platform_events"


class EventEmitter(object):
	"""
	Central event emitter for ES_Platform.

	Rules:
	- One Mongo doc per event
	- NEVER mutates state
	- Used by transitions, commands, orchestration
	"""

	def __init__(self, store):
		self.store = store
		self.mongo = store.mongo
		self.site_tz_id = store.site_tz_id
		self.logger = store.logger

	def emit(self,
			eventType,
			entityType,
			entityId,
			userId=None,
			eventId=None,
			details=None,
			context=None,
			corrId=None):
		"""
		entityType: SYSTEM | CARRIER | CHUTE | CMD
		entityId: systemCode | carrierId | chuteId | commandId
		"""
		ts = clock.pack_timestamps(tz_id=self.site_tz_id)

		doc = {
			"systemCode": self.store.systemCode,

			"eventType": str(eventType),
			"entityType": str(entityType),
			"entityId": entityId,

			"userId": userId,
			"eventId": eventId,
			"corrId": corrId or eventId,

			"details": dict(details or {}),

			# Auth overlay (optional)
			"authUser": _ctx(context, "authUser"),
			"authSource": _ctx(context, "authSource"),
			"roles": _ctx(context, "roles"),

			"tsEpoch": ts.get("tsEpoch"),
			"tsLocal": ts.get("tsLocal"),
			"tsUtc": ts.get("tsUtc"),
			"tzId": ts.get("tzId"),
		}

		try:
			self.mongo.insert_one(COL_EVENTS, doc)
		except Exception as e:
			if self.logger:
				self.logger.warn("EventEmitter.emit failed", {"err": str(e), "doc": doc})
		
		try:
			if getattr(self.store, "flight", None):
				self.store.flight.record_event(doc, level="INFO")
		except:
			pass

		return doc


def _ctx(context, key):
	if isinstance(context, dict):
		return context.get(key)
	return None