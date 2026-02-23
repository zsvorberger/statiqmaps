// === Route Builder main script (Leaflet + ORS + Elevation + Road Preview hook) ===

// Your ORS key is fetched from Flask (route: /get_ors_key)
let ORS_KEY = "";
fetch("/get_ors_key")
  .then(r => r.json())
  .then(data => { ORS_KEY = data.key || ""; })
  .catch(() => console.warn("Could not fetch ORS key"));

// --- Map init ---
const map = L.map("map").setView([40.4406, -79.9959], 12); // Pittsburgh center
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 18,
  attribution: "&copy; OpenStreetMap"
}).addTo(map);

// --- State ---
let markers = [];            // [ [lng,lat], [lng,lat], ... ] (ORS expects [lng,lat])
let markerLayers = [];       // Leaflet marker instances
let routeLine = null;        // Leaflet GeoJSON layer

// --- UI refs ---
const distanceDisplay = document.getElementById("distance");
const elevationCtx = document.getElementById("elevationChart").getContext("2d");

// --- Elevation chart ---
let elevationChart = new Chart(elevationCtx, {
  type: "line",
  data: {
    labels: [],
    datasets: [{
      label: "Elevation (m)",
      data: [],
      fill: true,
      tension: 0.1,
      borderWidth: 2
    }]
  },
  options: {
    scales: {
      x: { display: false },
      y: { beginAtZero: true }
    }
  }
});

// --- Helpers ---
function addMarker(lat, lng) {
  const ll = [lng, lat];          // ORS needs [lng, lat]
  markers.push(ll);
  const m = L.marker([lat, lng]).addTo(map);
  markerLayers.push(m);
}

function clearRouteLayer() {
  if (routeLine) {
    map.removeLayer(routeLine);
    routeLine = null;
  }
}

function fitToGeoJSON(fc) {
  try {
    const layer = L.geoJSON(fc);
    const b = layer.getBounds();
    if (b.isValid()) map.fitBounds(b, { padding: [40, 40] });
    map.removeLayer(layer);
  } catch {}
}

function kmString(km) {
  return (Math.round(km * 100) / 100).toFixed(2);
}

// --- Main: map click to build a route between two clicked points ---
map.on("click", async (e) => {
  const lat = e.latlng.lat;
  const lng = e.latlng.lng;

  addMarker(lat, lng);

  // Need at least 2 points to request a route
  if (markers.length < 2 || ORS_KEY === "") return;

  try {
    // Build ORS directions request (cycling-regular)
    const directionsRes = await fetch("https://api.openrouteservice.org/v2/directions/cycling-regular/geojson", {
      method: "POST",
      headers: {
        "Authorization": ORS_KEY,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ coordinates: markers })
    });

    if (!directionsRes.ok) {
      console.error("ORS directions error:", await directionsRes.text());
      return;
    }

    const directionsData = await directionsRes.json();

    // Draw/replace route on map
    clearRouteLayer();
    routeLine = L.geoJSON(directionsData).addTo(map);
    fitToGeoJSON(directionsData);

    // Distance (km) from ORS summary
    const distanceKm = (directionsData.features?.[0]?.properties?.summary?.distance || 0) / 1000;
    distanceDisplay.textContent = `${kmString(distanceKm)} km`;

    // Elevation profile request using the route geometry coords (lon,lat)
    const elevationCoords = directionsData.features?.[0]?.geometry?.coordinates || [];

    if (elevationCoords.length > 1) {
      const elevationRes = await fetch("https://api.openrouteservice.org/elevation/line", {
        method: "POST",
        headers: {
          "Authorization": ORS_KEY,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          format_in: "geojson",
          format_out: "json",
          geometry: {
            type: "LineString",
            coordinates: elevationCoords
          }
        })
      });

      if (!elevationRes.ok) {
        console.error("ORS elevation error:", await elevationRes.text());
      } else {
        const elevationData = await elevationRes.json();
        const elevations = elevationData.geometry.coordinates.map(coord => coord[2]);

        elevationChart.data.labels = elevations.map((_, i) => i);
        elevationChart.data.datasets[0].data = elevations;
        elevationChart.update();
      }
    } else {
      // No line? clear chart
      elevationChart.data.labels = [];
      elevationChart.data.datasets[0].data = [];
      elevationChart.update();
    }

    // --- Road Preview hook: send the route line to road_preview.js for surface breakdown
    // (This expects road_preview.js to be included in the HTML and a global map variable.)
    window._lastRouteLine = { type: "LineString", coordinates: directionsData.features?.[0]?.geometry?.coordinates || [] };
    if (typeof window.roadPreviewRouteUpdated === "function") {
      window.roadPreviewRouteUpdated(window._lastRouteLine);
    }

  } catch (err) {
    console.error("Routing error:", err);
  }
});

// --- Optional: simple keyboard shortcuts ---
// Press 'r' to reset markers and route
document.addEventListener("keydown", (ev) => {
  if (ev.key.toLowerCase() === "r") {
    // clear markers
    markerLayers.forEach(m => map.removeLayer(m));
    markerLayers = [];
    markers = [];
    // clear route
    clearRouteLayer();
    // clear distance + chart
    distanceDisplay.textContent = "0 km";
    elevationChart.data.labels = [];
    elevationChart.data.datasets[0].data = [];
    elevationChart.update();
    // clear surface stats (if road preview loaded)
    if (typeof window.roadPreviewRouteUpdated === "function") {
      window._lastRouteLine = null;
      window.roadPreviewRouteUpdated(null);
    }
  }
});
