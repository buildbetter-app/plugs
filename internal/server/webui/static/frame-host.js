(function () {
  "use strict";

  // This document contains only trusted host code. The plugin retains its
  // own opaque sandbox and unchanged digest-checked document/assets.
  if (parent === window) return;
  const entry = new URL(document.body.dataset.entry);
  if (entry.origin !== location.origin || entry.search || entry.hash) return;
  const frame = document.createElement("iframe");
  frame.title = "Plugin";
  frame.setAttribute("sandbox", "allow-scripts");
  frame.setAttribute("referrerpolicy", "no-referrer");
  frame.style.width = "100%";
  frame.style.height = "100%";
  frame.style.border = "0";
  document.body.style.margin = "0";
  let active = true, loads = 0;
  const revoke = () => {
    if (!active) return;
    active = false;
    document.documentElement.dataset.bridgeState = "revoked";
    parent.postMessage({ plugFrameLifecycle: "revoked" }, location.origin);
    frame.remove();
  };
  const onMessage = (event) => {
    if (!active) return;
    if (event.source === frame.contentWindow && event.origin === "null") {
      const request = event.data;
      const refuse = (id, code, message) => frame.contentWindow.postMessage({ jsonrpc: "2.0", id, error: { code: -32000, message, data: { code } } }, "*");
      if (!request || request.jsonrpc !== "2.0") { refuse(null, "invalid_message", "Bridge request envelope is invalid."); return; }
      let size;
      try { size = JSON.stringify(request).length; } catch (_) { refuse(null, "invalid_message", "Bridge message is not serializable."); return; }
      if (size > 256 * 1024) {
        const id = typeof request.id === "string" || (typeof request.id === "number" && Number.isFinite(request.id)) ? request.id : null;
        refuse(id, "message_too_large", "Bridge message exceeds the framing limit.");
        return;
      }
      // Rebuild the envelope: plugin input cannot forge host lifecycle events.
      parent.postMessage({ jsonrpc: request.jsonrpc, id: request.id, method: request.method, params: request.params }, location.origin);
    } else if (event.source === parent && event.origin === location.origin) {
      frame.contentWindow.postMessage(event.data, "*");
    }
  };
  addEventListener("message", onMessage);
  // Containment precedes execution of any other document. Load/pagehide are
  // additional permanent revocations, not the navigation security boundary.
  addEventListener("securitypolicyviolation", (event) => { if (event.effectiveDirective === "frame-src") revoke(); });
  addEventListener("pagehide", revoke, { once: true });
  frame.addEventListener("load", () => { if (++loads > 1) revoke(); });
  frame.src = entry.href;
  document.body.appendChild(frame);
}());
