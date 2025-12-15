# shared/es_platform/domain/transitions.py
# Refactored to use store.fast.* for speed (no Mongo re-reads)
# Adds chute-to-chute transfer helper (single eventId breadcrumbs on both chutes)

from shared.foundation.time import clock


class CarrierTransitions(object):
	def __init__(self, store):
		self.store = store

	# ----------------------------
	# Internal helpers
	# ----------------------------

	def _carrier_on_insert(self, carrierId, ts):
		try:
			base = self.store._build_carrier_doc(int(carrierId), ts) or {}
		except:
			base = {}

		if "_id" in base:
			try:
				del base["_id"]
			except:
				pass

		base.setdefault("systemCode", self.store.systemCode)
		base.setdefault("entityClass", "SORTER_CARRIER")
		base.setdefault("carrierId", int(carrierId))

		return base

	def assign(self, carrierId, assignedDest, ibn=None, order=None, inductionDevice=None, userId=None, eventId=None, details=None):
		if self.store.enable_cache:
			self.store.ensure_period_cache(hydrate=True)

		cid = int(carrierId)
		dst = str(assignedDest) if assignedDest is not None else None
		ts = clock.pack_timestamps(tz_id=self.store.site_tz_id)

		d = dict(details or {})
		if ibn is not None:
			d["ibn"] = str(ibn)
		if order is not None:
			d["order"] = str(order)
		if inductionDevice is not None:
			d["inductionDevice"] = str(inductionDevice)

		if d.get("location") is None and dst is not None:
			d["location"] = dst

		set_fields = {
			"currentPhase": "ASSIGNED",
			"assignedDest": dst,
			"lastLocation": d.get("location"),
			"lastSeenAtEpoch": ts.get("tsEpoch"),
			"lastEventType": "CARRIER_ASSIGNED",
			"lastEventId": eventId,
			"lastUserId": userId,
			"lastEventDetails": d,
		}
		if inductionDevice is not None:
			set_fields["inductionDevice"] = str(inductionDevice)

		self.store.fast.carrier_update(
			cid,
			set_fields=set_fields,
			inc_fields=None,
			set_on_insert=self._carrier_on_insert(cid, ts)
		)

		if dst:
			self.store.chute_mark_event(dst, "CARRIER_ASSIGNED_TO_CHUTE", details={
				"carrierId": cid,
				"assignedDest": dst,
				"ibn": d.get("ibn"),
				"order": d.get("order"),
			}, userId=userId, eventId=eventId)

		return {"ok": True, "carrierId": cid, "assignedDest": dst, "phase": "ASSIGNED", "ts": ts}

	def discharge_attempted(self, carrierId, location=None, userId=None, eventId=None, details=None):
		if self.store.enable_cache:
			self.store.ensure_period_cache(hydrate=True)

		cid = int(carrierId)
		ts = clock.pack_timestamps(tz_id=self.store.site_tz_id)

		d = dict(details or {})
		if location is not None:
			d["location"] = str(location)

		set_fields = {
			"currentPhase": "DISCHARGE_ATTEMPTED",
			"lastLocation": d.get("location"),
			"lastSeenAtEpoch": ts.get("tsEpoch"),
			"lastEventType": "DISCHARGE_ATTEMPTED",
			"lastEventId": eventId,
			"lastUserId": userId,
			"lastEventDetails": d,
		}
		inc = {"attemptedDeliveryCount": 1}

		self.store.fast.carrier_update(
			cid,
			set_fields=set_fields,
			inc_fields=inc,
			set_on_insert=self._carrier_on_insert(cid, ts)
		)

		return {"ok": True, "carrierId": cid, "phase": "DISCHARGE_ATTEMPTED", "ts": ts}

	def at_dest(self, carrierId, location=None, userId=None, eventId=None, details=None):
		if self.store.enable_cache:
			self.store.ensure_period_cache(hydrate=True)

		cid = int(carrierId)
		ts = clock.pack_timestamps(tz_id=self.store.site_tz_id)

		d = dict(details or {})
		if location is not None:
			d["location"] = str(location)

		set_fields = {
			"currentPhase": "AT_DEST",
			"lastLocation": d.get("location"),
			"lastSeenAtEpoch": ts.get("tsEpoch"),
			"lastEventType": "AT_DEST",
			"lastEventId": eventId,
			"lastUserId": userId,
			"lastEventDetails": d,
		}

		self.store.fast.carrier_update(
			cid,
			set_fields=set_fields,
			inc_fields=None,
			set_on_insert=self._carrier_on_insert(cid, ts)
		)

		return {"ok": True, "carrierId": cid, "phase": "AT_DEST", "ts": ts}

	def discharged_at_destination(self, carrierId, confirmedLocation=None, userId=None, eventId=None, details=None, clear_induction=True):
		if self.store.enable_cache:
			self.store.ensure_period_cache(hydrate=True)

		cid = int(carrierId)
		ts = clock.pack_timestamps(tz_id=self.store.site_tz_id)

		d = dict(details or {})
		if confirmedLocation is not None:
			d["confirmedLocation"] = str(confirmedLocation)

		set_fields = {
			"currentPhase": "DISCHARGED_AT_DESTINATION",
			"lastLocation": d.get("confirmedLocation"),
			"lastSeenAtEpoch": ts.get("tsEpoch"),
			"lastEventType": "DISCHARGED_AT_DESTINATION",
			"lastEventId": eventId,
			"lastUserId": userId,
			"lastEventDetails": d,
		}

		if clear_induction:
			set_fields["inductionDevice"] = None

		self.store.fast.carrier_update(
			cid,
			set_fields=set_fields,
			inc_fields=None,
			set_on_insert=self._carrier_on_insert(cid, ts)
		)

		return {"ok": True, "carrierId": cid, "phase": "DISCHARGED_AT_DESTINATION", "ts": ts}

	def recirculated(self, carrierId, inductionDevice=None, userId=None, eventId=None, details=None):
		if self.store.enable_cache:
			self.store.ensure_period_cache(hydrate=True)

		cid = int(carrierId)
		ts = clock.pack_timestamps(tz_id=self.store.site_tz_id)

		d = dict(details or {})
		if inductionDevice is not None:
			d["inductionDevice"] = str(inductionDevice)

		set_fields = {
			"currentPhase": "REASSIGNED",
			"lastSeenAtEpoch": ts.get("tsEpoch"),
			"lastEventType": "RECIRCULATED",
			"lastEventId": eventId,
			"lastUserId": userId,
			"lastEventDetails": d,
		}
		if inductionDevice is not None:
			set_fields["inductionDevice"] = str(inductionDevice)

		inc = {"recircCount": 1}

		self.store.fast.carrier_update(
			cid,
			set_fields=set_fields,
			inc_fields=inc,
			set_on_insert=self._carrier_on_insert(cid, ts)
		)

		return {"ok": True, "carrierId": cid, "phase": "REASSIGNED", "ts": ts}

	def abort(self, carrierId, reason, location=None, userId=None, eventId=None, details=None):
		if self.store.enable_cache:
			self.store.ensure_period_cache(hydrate=True)

		cid = int(carrierId)
		ts = clock.pack_timestamps(tz_id=self.store.site_tz_id)

		r = str(reason or "UNKNOWN").strip().upper().replace(" ", "_")
		phase = "ABORTED_%s" % r

		d = dict(details or {})
		d["reason"] = r
		if location is not None:
			d["location"] = str(location)

		set_fields = {
			"currentPhase": phase,
			"lastLocation": d.get("location"),
			"lastSeenAtEpoch": ts.get("tsEpoch"),
			"lastEventType": "ABORTED",
			"lastEventId": eventId,
			"lastUserId": userId,
			"lastEventDetails": d,
		}

		self.store.fast.carrier_update(
			cid,
			set_fields=set_fields,
			inc_fields=None,
			set_on_insert=self._carrier_on_insert(cid, ts)
		)

		return {"ok": True, "carrierId": cid, "phase": phase, "ts": ts}

	def reassign(self, carrierId, newDest, userId=None, eventId=None, details=None):
		if self.store.enable_cache:
			self.store.ensure_period_cache(hydrate=True)

		cid = int(carrierId)
		dst = str(newDest) if newDest is not None else None
		ts = clock.pack_timestamps(tz_id=self.store.site_tz_id)

		d = dict(details or {})
		d["newDest"] = dst

		set_fields = {
			"currentPhase": "REASSIGNED",
			"assignedDest": dst,
			"lastLocation": dst,
			"lastSeenAtEpoch": ts.get("tsEpoch"),
			"lastEventType": "REASSIGNED",
			"lastEventId": eventId,
			"lastUserId": userId,
			"lastEventDetails": d,
		}

		self.store.fast.carrier_update(
			cid,
			set_fields=set_fields,
			inc_fields=None,
			set_on_insert=self._carrier_on_insert(cid, ts)
		)

		if dst:
			self.store.chute_mark_event(dst, "CARRIER_REASSIGNED_TO_CHUTE", details={
				"carrierId": cid,
				"newDest": dst
			}, userId=userId, eventId=eventId)

		return {"ok": True, "carrierId": cid, "assignedDest": dst, "phase": "REASSIGNED", "ts": ts}


class ChuteTransitions(object):
	def __init__(self, store):
		self.store = store

	# ----------------------------
	# Internal helpers
	# ----------------------------

	def _chute_on_insert(self, chuteId, ts):
		try:
			base = self.store._build_chute_doc(str(chuteId), ts) or {}
		except:
			base = {}

		if "_id" in base:
			try:
				del base["_id"]
			except:
				pass

		base.setdefault("systemCode", self.store.systemCode)
		base.setdefault("entityClass", "SORTER_CHUTE")
		base.setdefault("chuteId", str(chuteId))

		return base

	def _cached_chute(self, chuteId):
		"""
		Best-effort read from in-memory only (no Mongo read).
		"""
		try:
			return (self.store._chutes or {}).get(str(chuteId))
		except:
			return None

	# ----------------------------
	# Flags / lifecycle
	# ----------------------------

	def enable(self, chuteId, userId=None, eventId=None, details=None):
		return self._set_flags(chuteId, enabled=True, userId=userId, eventId=eventId, eventType="CHUTE_ENABLED", details=details)

	def disable(self, chuteId, userId=None, eventId=None, details=None):
		return self._set_flags(chuteId, enabled=False, userId=userId, eventId=eventId, eventType="CHUTE_DISABLED", details=details)

	def fault(self, chuteId, faulted=True, userId=None, eventId=None, details=None):
		return self._set_flags(chuteId, faulted=bool(faulted), userId=userId, eventId=eventId, eventType="CHUTE_FAULT", details=details)

	# ----------------------------
	# Occupancy
	# ----------------------------

	def occupy(self, chuteId, carrierId=None, ibn=None, order=None, userId=None, eventId=None, details=None):
		if self.store.enable_cache:
			self.store.ensure_period_cache(hydrate=True)

		chuteId = str(chuteId)
		ts = clock.pack_timestamps(tz_id=self.store.site_tz_id)

		d = dict(details or {})
		if carrierId is not None:
			d["carrierId"] = int(carrierId)
		if ibn is not None:
			d["ibn"] = str(ibn)
		if order is not None:
			d["order"] = str(order)

		set_fields = {
			"occupied": True,
			"lastCarrierId": d.get("carrierId"),
			"lastIbn": d.get("ibn"),
			"lastOrder": d.get("order"),
		}
		inc = {"occupancyCount": 1}

		self.store.fast.chute_update(
			chuteId,
			set_fields=set_fields,
			inc_fields=inc,
			set_on_insert=self._chute_on_insert(chuteId, ts)
		)

		self.store.chute_mark_event(chuteId, "CHUTE_OCCUPIED", details=d, userId=userId, eventId=eventId)

		return {"ok": True, "chuteId": chuteId, "occupied": True, "ts": ts}

	def release(self, chuteId, userId=None, eventId=None, details=None):
		if self.store.enable_cache:
			self.store.ensure_period_cache(hydrate=True)

		chuteId = str(chuteId)
		ts = clock.pack_timestamps(tz_id=self.store.site_tz_id)
		d = dict(details or {})

		self.store.fast.chute_update(
			chuteId,
			set_fields={"occupied": False},
			inc_fields=None,
			set_on_insert=self._chute_on_insert(chuteId, ts)
		)

		self.store.chute_mark_event(chuteId, "CHUTE_RELEASED", details=d, userId=userId, eventId=eventId)

		return {"ok": True, "chuteId": chuteId, "occupied": False, "ts": ts}

	# ----------------------------
	# Assignment metadata
	# ----------------------------

	def assign_name(self, chuteId, assignedName, assignedMode=None, userId=None, eventId=None, details=None):
		if self.store.enable_cache:
			self.store.ensure_period_cache(hydrate=True)

		chuteId = str(chuteId)
		ts = clock.pack_timestamps(tz_id=self.store.site_tz_id)

		d = dict(details or {})
		d["assignedName"] = str(assignedName) if assignedName is not None else None
		if assignedMode is not None:
			d["assignedMode"] = str(assignedMode)

		set_fields = {
			"assignedName": d.get("assignedName"),
			"assignedMode": d.get("assignedMode"),
		}

		self.store.fast.chute_update(
			chuteId,
			set_fields=set_fields,
			inc_fields=None,
			set_on_insert=self._chute_on_insert(chuteId, ts)
		)

		self.store.chute_mark_event(chuteId, "CHUTE_ASSIGNED_NAME", details=d, userId=userId, eventId=eventId)

		return {"ok": True, "chuteId": chuteId, "assignedName": d.get("assignedName"), "assignedMode": d.get("assignedMode"), "ts": ts}

	# ----------------------------
	# Transfer helper (NEW)
	# ----------------------------

	def transfer(self, sourceChuteId, destChuteId, carrierId=None, ibn=None, order=None, userId=None, eventId=None, details=None):
		"""
		Move “contents” from one chute to another (operator tool / correction).

		Notes:
		- No Mongo reads. If carrierId/ibn/order not provided, best-effort pulls from in-memory cache only.
		- Updates both chute docs with the same eventId for easy correlation.
		- Does NOT automatically change a carrier doc unless carrierId is known (then it will reassign it).
		"""
		if self.store.enable_cache:
			self.store.ensure_period_cache(hydrate=True)

		src = str(sourceChuteId)
		dst = str(destChuteId)
		ts = clock.pack_timestamps(tz_id=self.store.site_tz_id)

		d = dict(details or {})
		d["sourceChuteId"] = src
		d["destChuteId"] = dst

		# Best-effort infer from cache
		if carrierId is None or ibn is None or order is None:
			cached = self._cached_chute(src) or {}
			if carrierId is None:
				try:
					carrierId = cached.get("lastCarrierId")
				except:
					pass
			if ibn is None:
				try:
					ibn = cached.get("lastIbn")
				except:
					pass
			if order is None:
				try:
					order = cached.get("lastOrder")
				except:
					pass

		if carrierId is not None:
			try:
				d["carrierId"] = int(carrierId)
			except:
				d["carrierId"] = carrierId
		if ibn is not None:
			d["ibn"] = str(ibn)
		if order is not None:
			d["order"] = str(order)

		# 1) Release source chute
		self.store.fast.chute_update(
			src,
			set_fields={
				"occupied": False,
				"lastEventType": "CHUTE_TRANSFER_OUT",
				"lastEventId": eventId,
				"lastUserId": userId,
				"lastEventDetails": d,
			},
			inc_fields=None,
			set_on_insert=self._chute_on_insert(src, ts)
		)

		# 2) Occupy destination chute
		self.store.fast.chute_update(
			dst,
			set_fields={
				"occupied": True,
				"lastCarrierId": d.get("carrierId"),
				"lastIbn": d.get("ibn"),
				"lastOrder": d.get("order"),
				"lastEventType": "CHUTE_TRANSFER_IN",
				"lastEventId": eventId,
				"lastUserId": userId,
				"lastEventDetails": d,
			},
			inc_fields={"occupancyCount": 1},
			set_on_insert=self._chute_on_insert(dst, ts)
		)

		# 3) Breadcrumb events on both
		self.store.chute_mark_event(src, "CHUTE_TRANSFER_OUT", details=d, userId=userId, eventId=eventId)
		self.store.chute_mark_event(dst, "CHUTE_TRANSFER_IN", details=d, userId=userId, eventId=eventId)

		# 4) If we know the carrier, reassign it to the new chute (fast, no read)
		if carrierId is not None:
			try:
				cid = int(carrierId)
				self.store.fast.carrier_update(cid, set_fields={
					"assignedDest": dst,
					"lastLocation": dst,
					"lastEventType": "CARRIER_REASSIGNED_BY_TRANSFER",
					"lastEventId": eventId,
					"lastUserId": userId,
					"lastEventDetails": d,
				}, inc_fields=None, set_on_insert=self.store.carriers._carrier_on_insert(cid, ts))
			except:
				pass

		return {"ok": True, "sourceChuteId": src, "destChuteId": dst, "carrierId": d.get("carrierId"), "ts": ts}

	# ----------------------------
	# Flags helper
	# ----------------------------

	def _set_flags(self, chuteId, enabled=None, faulted=None, userId=None, eventId=None, eventType="CHUTE_FLAG", details=None):
		if self.store.enable_cache:
			self.store.ensure_period_cache(hydrate=True)

		chuteId = str(chuteId)
		ts = clock.pack_timestamps(tz_id=self.store.site_tz_id)
		d = dict(details or {})

		fields = {}
		if enabled is not None:
			fields["enabled"] = bool(enabled)
		if faulted is not None:
			fields["faulted"] = bool(faulted)

		self.store.fast.chute_update(
			chuteId,
			set_fields=fields,
			inc_fields=None,
			set_on_insert=self._chute_on_insert(chuteId, ts)
		)

		self.store.chute_mark_event(chuteId, str(eventType), details=d, userId=userId, eventId=eventId)

		return {"ok": True, "chuteId": chuteId, "fields": fields, "ts": ts}
		
		
		
#		
## Move whatever is “in” DST-0012-1-1-A to DST-0013-1-1-A (best-effort uses cache for carrier/ibn/order)
#store.chutes.transfer("DST-0012-1-1-A", "DST-0013-1-1-A", userId="joe", eventId="XFER-0001")
#
## Or explicit (recommended if you have it)
#store.chutes.transfer("DST-0012-1-1-A", "DST-0013-1-1-A", carrierId=12, ibn="476JB6", order="123456789", userId="joe", eventId="XFER-0002")