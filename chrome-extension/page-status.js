const hash = new URLSearchParams(location.hash.replace(/^#/, ""));

const reason = hash.get("reason");
if (reason) {
  const node = document.getElementById("reason");
  if (node) {
    node.textContent = decodeURIComponent(reason);
  }
}

const message = hash.get("message");
if (message) {
  const node = document.getElementById("message");
  if (node) {
    node.textContent = decodeURIComponent(message);
  }
}
