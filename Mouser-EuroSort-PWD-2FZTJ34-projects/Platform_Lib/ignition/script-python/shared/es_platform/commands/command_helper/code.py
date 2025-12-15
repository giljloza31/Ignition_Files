# shared/es_platform/commands/command_helper.py
# Drop-in CommandHelper wired to FlightRecorder + permissions + receipts + optional queue
# Jython-safe

from shared.foundation.time import clock
from shared.es_platform.commands import tagmap
from shared.es_platform.commands.queue import CommandQueue
from shared.es_platform.commands.permissions import CommandAuthorizer, PermissionDenied, default_rules
from shared.es_platform.commands.receipt_store import ReceiptStore


class CommandHelper(object):
	"""
	High-level command helper for operator tools.

	Adds:
	- Permission guard (context["roles"] or user roles)
	- Optional queue (throttle + dedupe)
	- Command receipts persisted to Mongo
	- Dry-run support (no tag writes, still returns commandId)
	- Mongo breadcrumbs via StateStore (chute_mark_event / carrier updates)
	- FlightRecorder hooks (request/sent/ack/fail/deny)
	- UI-friendly errors for PermissionDenied
	"""

	def __init__(self,
			systemCode,
			state_store,
			tag_writer=None,
			site_tz_id="UTC",
			dry_run=False,
			logger=None,

			# receipts / queue
			receipt_store=None,
			use_queue=False,
			queue=None,
			default_timeout_ms=5000,

			# auth
			authorizer=None,
			auth_rules=None,
			default_allow=False,
			user_sources=None):
		self.systemCode = str(systemCode)
		self.store = state_store
		self.tag_writer = tag_writer or IgnitionTagWriter()
		self.site_tz_id = str(site_tz_id or "UTC")
		self.dry_run = bool(dry_run)
		self.logger = logger

		self.default_timeout_ms = int(default_timeout_ms)

		# Receipts
		self.receipts = receipt_store or ReceiptStore(self.store.mongo, site_tz_id=self.site_tz_id)

		# Queue
		self.use_queue = bool(use_queue)
		self.queue = queue or CommandQueue(logger=logger, site_tz_id=self.site_tz_id)

		# Authorizer
		if authorizer:
			self.authorizer = authorizer
		else:
			rules = auth_rules if auth_rules is not None else default_rules()
			self.authorizer = CommandAuthorizer(
				rules=rules,
				default_allow=default_allow,
				user_sources=user_sources or ["Active Directory", "Ignition"],
				site_tz_id=self.site_tz_id,
				logger=logger
			)

	# ----------------------------
	# Logging (logger + flight recorder + print fallback)
	# ----------------------------

	def _flight(self):
		try:
			return getattr(self.store, "flight", None)
		except:
			return None

	def _log(self, msg, payload=None, level="info", eventType=None, entityType="COMMAND", entityId=None, userId=None, eventId=None):
		# 1) app logger
		if self.logger:
			try:
				fn = getattr(self.logger, level, None)
				if fn:
					fn(msg, payload)
			except:
				pass

		# 2) flight recorder
		try:
			fl = self._flight()
			if fl:
				fl.record(
					level=str(level or "INFO").upper(),
					message=str(msg),
					payload=payload,
					eventType=eventType,
					entityType=entityType,
					entityId=entityId,
					userId=userId,
					eventId=eventId,
					corrId=(entityId or eventId)
				)
		except:
			pass

		# 3) print fallback
		try:
			print("%s %s" % (msg, payload if payload is not None else ""))
		except:
			pass

	# ----------------------------
	# Permission + UI-safe wrapping
	# ----------------------------

	def _authorize(self, eventType, userId=None, context=None):
		"""
		Raises PermissionDenied if not allowed.
		"""
		return self.authorizer.require(eventType, userId=userId, context=context)

	def _deny_payload(self, e, eventType, userId=None, context=None):
		p = {}
		try:
			p.update(getattr(e, "payload", {}) or {})
		except:
			pass

		p.setdefault("eventType", str(eventType))
		p.setdefault("userId", userId)
		if isinstance(context, dict):
			if context.get("authUser"):
				p.setdefault("authUser", context.get("authUser"))
			if context.get("authSource"):
				p.setdefault("authSource", context.get("authSource"))
			if context.get("roles") is not None:
				p.setdefault("authRoles", context.get("roles"))
		return p

	# ----------------------------
	# Breadcrumbs (StateStore)
	# ----------------------------

	def _record(self, chuteId=None, carrierId=None, eventType="CMD", details=None, userId=None, eventId=None, context=None):
		d = dict(details or {})

		# Stamp re-auth info if present
		if isinstance(context, dict):
			if context.get("authUser"):
				d["authUser"] = context.get("authUser")
			if context.get("authSource"):
				d["authSource"] = context.get("authSource")
			if context.get("roles") is not None:
				d["authRoles"] = context.get("roles")

		if chuteId is not None:
			self.store.chute_mark_event(str(chuteId), eventType, details=d, userId=userId, eventId=eventId)

		if carrierId is not None:
			self.store.upsert_carrier(int(carrierId), fields={
				"lastEventType": eventType,
				"lastEventId": eventId,
				"lastUserId": userId,
				"lastEventDetails": d,
			}, inc=None, on_insert={"createdAt": clock.pack_timestamps(tz_id=self.site_tz_id), "entityClass": "SORTER_CARRIER"})

	# ----------------------------
	# Writes + receipts
	# ----------------------------

	def _write_now(self, writes, userId=None, eventId=None, eventType="CMD"):
		ts = clock.pack_timestamps(tz_id=self.site_tz_id)
		payload = {"writes": writes, "dry_run": self.dry_run, "ts": ts, "userId": userId, "eventId": eventId, "eventType": eventType}

		self._log("CommandHelper.write_now", payload, level="info",
			eventType=eventType, entityType="COMMAND", entityId=None, userId=userId, eventId=eventId)

		if self.dry_run:
			return {"ok": True, "dry_run": True, "writes": writes, "ts": ts}

		res = self.tag_writer.write(writes)
		return {"ok": True, "dry_run": False, "writes": writes, "result": res, "ts": ts}

	def _new_receipt(self, eventType, writes, userId=None, eventId=None, context=None, chuteId=None, carrierId=None, dedupe_key=None):
		commandId = None
		try:
			if self.receipts:
				commandId = self.receipts.new_command_id(self.systemCode)
				self.receipts.create_receipt(
					commandId=commandId,
					systemCode=self.systemCode,
					eventType=eventType,
					writes=writes,
					userId=userId,
					eventId=eventId,
					context=context,
					chuteId=chuteId,
					carrierId=carrierId,
					dedupe_key=dedupe_key
				)
		except Exception as e:
			self._log("CommandHelper._new_receipt failed", {"err": str(e)}, level="warn",
				eventType="CMD_RECEIPT_ERROR", entityType="SYSTEM", entityId=self.systemCode, userId=userId, eventId=eventId)
		return commandId

	def _write_with_receipt(self, commandId, writes, userId=None, eventId=None, eventType="CMD", timeout_ms=None):
		# Mark SENT
		if self.receipts and commandId:
			try:
				self.receipts.mark_sent(commandId)
			except:
				pass

		self._log("CMD_SENT", {"commandId": commandId, "writes": writes, "timeout_ms": timeout_ms, "dry_run": self.dry_run},
			level="info", eventType=eventType, entityType="COMMAND", entityId=commandId, userId=userId, eventId=eventId)

		try:
			res = self._write_now(writes, userId=userId, eventId=eventId, eventType=eventType)

			# Mark ACK (tag write success)
			if self.receipts and commandId and not res.get("dry_run"):
				try:
					self.receipts.mark_ack(commandId, write_result=res.get("result"))
				except:
					pass

			self._log("CMD_ACK", {"commandId": commandId, "result": res.get("result"), "dry_run": res.get("dry_run")},
				level="info", eventType=eventType, entityType="COMMAND", entityId=commandId, userId=userId, eventId=eventId)

			res["commandId"] = commandId
			return res

		except Exception as e:
			self._log("CMD_FAILED", {"commandId": commandId, "error": str(e)},
				level="error", eventType=eventType, entityType="COMMAND", entityId=commandId, userId=userId, eventId=eventId)

			if self.receipts and commandId:
				try:
					self.receipts.mark_failed(commandId, error_msg=str(e), write_result=None)
				except:
					pass
			raise

	# ----------------------------
	# Dispatcher (the wiring point)
	# ----------------------------

	def _dispatch(self, eventType, writes, userId=None, eventId=None, context=None,
			dedupe_key=None, chuteId=None, carrierId=None, timeout_ms=None):

		# 1) Permission guard first (UI-friendly)
		try:
			self._authorize(eventType, userId=userId, context=context)
		except PermissionDenied as e:
			payload = self._deny_payload(e, eventType, userId=userId, context=context)

			self._log("CMD_DENIED", payload, level="warn",
				eventType=eventType, entityType="COMMAND", entityId=None, userId=userId, eventId=eventId)

			return {"ok": False, "denied": True, "message": str(e), "payload": payload}

		to_ms = int(timeout_ms) if timeout_ms is not None else self.default_timeout_ms

		# 2) Receipt first
		commandId = self._new_receipt(eventType, writes, userId=userId, eventId=eventId, context=context,
			chuteId=chuteId, carrierId=carrierId, dedupe_key=dedupe_key)

		# 3) Breadcrumb (include commandId)
		self._record(chuteId=chuteId, carrierId=carrierId, eventType=eventType,
			details={"writes": writes, "commandId": commandId, "dedupe_key": dedupe_key},
			userId=userId, eventId=eventId, context=context)

		# 4) Flight: request
		self._log("CMD_REQUEST", {
				"commandId": commandId,
				"eventType": eventType,
				"writes": writes,
				"dedupe_key": dedupe_key,
				"chuteId": chuteId,
				"carrierId": carrierId,
				"dry_run": self.dry_run,
				"use_queue": self.use_queue
			},
			level="info", eventType=eventType, entityType="COMMAND", entityId=commandId, userId=userId, eventId=eventId)

		# 5) Queue path
		if self.use_queue and (not self.dry_run):
			item = {
				"commandId": commandId,
				"systemCode": self.systemCode,
				"eventType": eventType,
				"eventId": eventId,
				"writes": writes,
				"userId": userId,
				"context": context,
				"dedupe_key": dedupe_key,
				"chuteId": chuteId,
				"carrierId": carrierId,
				"timeout_ms": to_ms,
			}
			r = self.queue.enqueue(item)
			r["commandId"] = commandId

			self._log("CMD_QUEUED", {"commandId": commandId, "enqueue": r, "dedupe_key": dedupe_key},
				level="info", eventType=eventType, entityType="COMMAND", entityId=commandId, userId=userId, eventId=eventId)

			return r

		# 6) Immediate path (dry_run or no queue)
		return self._write_with_receipt(commandId, writes, userId=userId, eventId=eventId, eventType=eventType, timeout_ms=to_ms)

	# ----------------------------
	# Queue drain helpers (runner/timer)
	# ----------------------------

	def drain_queue_once(self):
		return self.queue.drain_once(self._queue_writer_fn)

	def drain_queue_all(self, max_items=50):
		return self.queue.drain_all(self._queue_writer_fn, max_items=max_items)

	def _queue_writer_fn(self, item):
		commandId = item.get("commandId")
		eventType = item.get("eventType")
		writes = item.get("writes") or []
		userId = item.get("userId")
		eventId = item.get("eventId")
		context = item.get("context")
		chuteId = item.get("chuteId")
		carrierId = item.get("carrierId")
		timeout_ms = item.get("timeout_ms")

		# Breadcrumb queued->sent
		self._record(chuteId=chuteId, carrierId=carrierId, eventType=eventType,
			details={"writes": writes, "queued": True, "commandId": commandId},
			userId=userId, eventId=eventId, context=context)

		self._log("CMD_DEQUEUED", {"commandId": commandId, "writes": writes},
			level="info", eventType=eventType, entityType="COMMAND", entityId=commandId, userId=userId, eventId=eventId)

		return self._write_with_receipt(commandId, writes, userId=userId, eventId=eventId, eventType=eventType, timeout_ms=timeout_ms)

	# ----------------------------
	# System commands
	# ----------------------------

	def system_on(self, userId=None, eventId=None, context=None):
		writes = [(tagmap.system_enable(self.systemCode), True)]
		return self._dispatch("CMD_SYSTEM_ON", writes, userId=userId, eventId=eventId, context=context, dedupe_key="SYSTEM_ON")

	def system_off(self, userId=None, eventId=None, context=None):
		writes = [(tagmap.system_disable(self.systemCode), True)]
		return self._dispatch("CMD_SYSTEM_OFF", writes, userId=userId, eventId=eventId, context=context, dedupe_key="SYSTEM_OFF")

	def set_mode(self, mode, userId=None, eventId=None, context=None):
		writes = [(tagmap.system_mode(self.systemCode), str(mode))]
		return self._dispatch("CMD_SET_MODE", writes, userId=userId, eventId=eventId, context=context, dedupe_key="SYSTEM_MODE", timeout_ms=5000)

	# ----------------------------
	# Chute commands
	# ----------------------------

	def open_chute_door(self, dst, userId=None, eventId=None, context=None):
		dst = str(dst)
		writes = [(tagmap.chute_door_open(self.systemCode, dst), True)]
		return self._dispatch("CMD_CHUTE_OPEN", writes, userId=userId, eventId=eventId, context=context,
			dedupe_key="OPEN:%s" % dst, chuteId=dst, timeout_ms=5000)

	def close_chute_door(self, dst, userId=None, eventId=None, context=None):
		dst = str(dst)
		writes = [(tagmap.chute_door_close(self.systemCode, dst), True)]
		return self._dispatch("CMD_CHUTE_CLOSE", writes, userId=userId, eventId=eventId, context=context,
			dedupe_key="CLOSE:%s" % dst, chuteId=dst, timeout_ms=5000)

	def set_chute_light(self, dst, on=True, userId=None, eventId=None, context=None):
		dst = str(dst)
		writes = [(tagmap.chute_light(self.systemCode, dst), bool(on))]
		return self._dispatch("CMD_CHUTE_LIGHT", writes, userId=userId, eventId=eventId, context=context,
			dedupe_key="LIGHT:%s:%s" % (dst, "1" if on else "0"), chuteId=dst, timeout_ms=2000)

	# ----------------------------
	# Carrier commands
	# ----------------------------

	def force_release_carrier(self, carrierId, userId=None, eventId=None, context=None):
		cid = int(carrierId)
		writes = [(tagmap.carrier_force_release(self.systemCode, cid), True)]
		return self._dispatch("CMD_CARRIER_FORCE_RELEASE", writes, userId=userId, eventId=eventId, context=context,
			dedupe_key="FORCE_RELEASE:%s" % cid, carrierId=cid, timeout_ms=5000)


class IgnitionTagWriter(object):
	"""
	Default writer using Ignition tag system.
	Works in Gateway scope. In Script Console, it will also work if system.tag is available.
	"""
	def write(self, writes):
		try:
			import system
			paths = [w[0] for w in writes]
			values = [w[1] for w in writes]
			return system.tag.writeBlocking(paths, values)
		except Exception as e:
			raise RuntimeError("Tag write failed: %s" % e)