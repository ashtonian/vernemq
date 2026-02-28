# Changelog

 - Bugfix: Close hackney connection reference on `hackney:body/1` error to
   prevent connection pool leaks.
 - Add periodic cache sweep to purge expired `auth_on_*` cache entries that
   would otherwise accumulate until looked up. Configurable via
   `vmq_webhooks.cache_sweep_interval` (default: 60 seconds).
 - Cache SSL options in `persistent_term` to eliminate repeated
   `application:get_env` calls (8 per HTTPS webhook invocation). Options are
   refreshed on each cache sweep cycle.
 - Pre-compute hook name binaries at startup to avoid per-request
   `atom_to_binary/2` allocations in the HTTP header path.
 - Dispatch fire-and-forget notification webhooks (`on_publish`,
   `on_register`, `on_subscribe`, `on_offline_message`, `on_client_*`,
   `on_session_expired`) asynchronously so they no longer block the
   calling FSM process. Bounded by `vmq_webhooks.async_pool_size`
   (default: 100); excess notifications are dropped with a warning.
 - Collect per webhook type (e.g. `on_publish_m5_requests`) metrics.
 - Move persistence of webhooks to the `vernemq.conf` main file. This means
   adding hooks using the `vmq-admin` tool no longer persists the webhooks and
   they have to be manually added to the `vernemq.conf` file.
 - Make it possible to reject individual topics when subscribing.

## vmq_webhooks 0.2.0

Backwards incompatible changes:

 - base64 encode MQTT payloads by default.
 - In all hooks `subscriber_id` has been renamed to `client_id` to be consistent
   with VerneMQ and other plugins where a `subscriber_id` is defined as a
   mountpoint and a client id.
 - `on_offline_message` now also passes `qos`, `topic`, `payload` and `retain`
   fields as part of the JSON message. Note, that this change **requires VerneMQ
   0.15.2 or newer to work**.

Other changes:

 - Webhooks can be persisted across broker restarts by adding them to the
   `priv/vmq_webhooks.conf` file.


## vmq_webhooks 0.1.0

Initial version.
