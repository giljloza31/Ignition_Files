# shared/es_platform/domain/fast_update.py
# Fast update helpers: write to Mongo and update in-memory cache without re-read

from shared.foundation.time import clock


class FastUpdate(object):
	def __init__(self, store):
		self.store = store

	# ----------------------------
	# Carrier fast update
	# ----------------------------

	def carrier_update(self, carrierId, set_fields=None, inc_fields=None, set_on_insert=None):
		"""
		Update Mongo + update cache in-place (no re-read).

		set_fields: dict -> $set
		inc_fields: dict -> $inc
		set_on_insert: dict -> $setOnInsert
		"""
		if self.store.enable_cache:
			self.store.ensure_period_cache(hydrate=True)

		cid = int(carrierId)
		pk = self.store._carrier_pk(cid)

		ts = clock.pack_timestamps(tz_id=self.store.site_tz_id)

		update = {"$set": {"updatedAtEpoch": ts.get("tsEpoch")}}
		if set_fields:
			update["$set"].update(dict(set_fields))
		if inc_fields:
			update["$inc"] = dict(inc_fields)
		if set_on_insert:
			update["$setOnInsert"] = dict(set_on_insert)

		self.store.mongo.update_one(self.store.COL_CARRIERS, {"_id": pk}, update, upsert=True)

		# Update cache in-place
		if self.store.enable_cache:
			doc = self.store._carriers.get(cid)
			if doc is None:
				doc = {"_id": pk, "systemCode": self.store.systemCode, "carrierId": cid, "entityClass": "SORTER_CARRIER"}
				self.store._carriers[cid] = doc

			# apply $set
			for k, v in update.get("$set", {}).items():
				doc[k] = v

			# apply $inc
			for k, v in update.get("$inc", {}).items():
				try:
					doc[k] = (doc.get(k) or 0) + v
				except:
					doc[k] = v

			# apply $setOnInsert only if truly new (createdAtEpoch missing)
			if set_on_insert and doc.get("createdAtEpoch") is None:
				for k, v in set_on_insert.items():
					doc[k] = v

		return {"ok": True, "carrierId": cid, "updatedAtEpoch": ts.get("tsEpoch")}

	# ----------------------------
	# Chute fast update
	# ----------------------------

	def chute_update(self, chuteId, set_fields=None, inc_fields=None, set_on_insert=None):
		"""
		Update Mongo + update cache in-place (no re-read).
		"""
		if self.store.enable_cache:
			self.store.ensure_period_cache(hydrate=True)

		chuteId = str(chuteId)
		pk = self.store._chute_pk(chuteId)

		ts = clock.pack_timestamps(tz_id=self.store.site_tz_id)

		update = {"$set": {"updatedAtEpoch": ts.get("tsEpoch")}}
		if set_fields:
			update["$set"].update(dict(set_fields))
		if inc_fields:
			update["$inc"] = dict(inc_fields)
		if set_on_insert:
			update["$setOnInsert"] = dict(set_on_insert)

		self.store.mongo.update_one(self.store.COL_CHUTES, {"_id": pk}, update, upsert=True)

		# Update cache in-place
		if self.store.enable_cache:
			doc = self.store._chutes.get(chuteId)
			if doc is None:
				doc = {"_id": pk, "systemCode": self.store.systemCode, "chuteId": chuteId, "entityClass": "SORTER_CHUTE"}
				self.store._chutes[chuteId] = doc

			# apply $set
			for k, v in update.get("$set", {}).items():
				doc[k] = v

			# apply $inc
			for k, v in update.get("$inc", {}).items():
				try:
					doc[k] = (doc.get(k) or 0) + v
				except:
					doc[k] = v

			# apply $setOnInsert only if truly new (createdAtEpoch missing)
			if set_on_insert and doc.get("createdAtEpoch") is None:
				for k, v in set_on_insert.items():
					doc[k] = v

		return {"ok": True, "chuteId": chuteId, "updatedAtEpoch": ts.get("tsEpoch")}

	# ----------------------------
	# Convenience wrappers matching StateStore semantics
	# ----------------------------

	def carrier_mark_event(self, carrierId, eventType, details=None, userId=None, eventId=None):
		"""
		Stores lastEvent fields on carrier doc (fast, cache-safe).
		"""
		d = dict(details or {})
		return self.carrier_update(int(carrierId), set_fields={
			"lastEventType": str(eventType),
			"lastEventId": eventId,
			"lastUserId": userId,
			"lastEventDetails": d,
		})

	def chute_mark_flags(self, chuteId, enabled=None, faulted=None, occupied=None):
		fields = {}
		if enabled is not None:
			fields["enabled"] = bool(enabled)
		if faulted is not None:
			fields["faulted"] = bool(faulted)
		if occupied is not None:
			fields["occupied"] = bool(occupied)

		return self.chute_update(str(chuteId), set_fields=fields)