const CACHE_NAME = "drone-chrono-pwa-v1";
const ASSETS = [
  "./sensor.html",
  "./admin.html",
  "./dashboard.html",
  "./css/styles.css",
  "./js/firebase-config.js",
  "./js/sensors.js"
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(ASSETS))
  );
  self.skipWaiting();
});

self.addEventListener("fetch", (event) => {
  event.respondWith(
    fetch(event.request).catch(() => caches.match(event.request))
  );
});
