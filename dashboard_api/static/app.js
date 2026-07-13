let simulatedProvinces = null;

// Inicialización del Mapa
// Centrado en Guayaquil, Ecuador para el foco principal del proyecto
const map = L.map('map', {
    zoomControl: false 
}).setView([-2.1962, -79.8862], 7); 

const darkLayer = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> contributors &copy; <a href="https://carto.com/">CARTO</a>',
    subdomains: 'abcd',
    maxZoom: 20
});

const satelliteLayer = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
    attribution: 'Tiles &copy; Esri',
    maxZoom: 19
});

// Iniciamos con el mapa oscuro por defecto
darkLayer.addTo(map);

// Lógica del botón de cambio de mapa
let isSatellite = false;
const btnToggleMap = document.getElementById('map-toggle-btn');
if (btnToggleMap) {
    btnToggleMap.addEventListener('click', () => {
        if (isSatellite) {
            map.removeLayer(satelliteLayer);
            darkLayer.addTo(map);
            btnToggleMap.innerHTML = '🌍 Satelital';
            isSatellite = false;
        } else {
            map.removeLayer(darkLayer);
            satelliteLayer.addTo(map);
            btnToggleMap.innerHTML = '🗺️ Mapa Oscuro';
            isSatellite = true;
        }
    });
}

L.control.zoom({ position: 'bottomright' }).addTo(map);

// Grupos de capas
let precipitationLayer = L.layerGroup().addTo(map);
let tideLayer = L.layerGroup().addTo(map);
let combinedRiskLayer = L.layerGroup().addTo(map);
let routeLayer = L.layerGroup().addTo(map); // Para la ruta OSRM

// Referencias a UI
const btnRefresh = document.getElementById('refresh-btn');
const togglePrecip = document.getElementById('layer-precipitation');
const toggleTide = document.getElementById('layer-tide');
const btnSimStorm = document.getElementById('btn-sim-storm');
const btnClearRoute = document.getElementById('btn-clear-route');
let isSimStormActive = false;
const txtTotalRed = document.getElementById('total-red');
const txtTotalYellow = document.getElementById('total-yellow');
const txtTotalBlue = document.getElementById('total-blue');
const txtTotalGreen = document.getElementById('total-green');
const txtLastUpdate = document.getElementById('last-update');

function getRainColor(alertLevel) {
    if (alertLevel.includes("CRÍTICO")) return "#f85149"; // Rojo
    if (alertLevel.includes("Moderada")) return "#d29922"; // Amarillo
    if (alertLevel.includes("Ligera")) return "#2f81f7"; // Azul claro
    return "#3fb950"; // Verde
}

function getTideColor(tideLevel) {
    if (tideLevel >= 3.5) return "#9e6a03"; // Naranja oscuro para pleamar
    if (tideLevel <= 1.5) return "#58a6ff"; // Azul para bajamar
    return "#8b949e"; // Gris normal
}

function getCombinedRiskColor(risk) {
    if (risk.includes("ALERTA ROJA")) return "#ff0000"; // Rojo intenso brillante
    if (risk.includes("CRÍTICO")) return "#f85149";
    if (risk.includes("Moderada")) return "#d29922";
    if (risk.includes("Ligera")) return "#2f81f7";
    return "#3fb950"; 
}

// Generador dinámico de zonas seguras por provincia
function getSafeZonesForProvince(baseLat, baseLon, provincia) {
    if (provincia === "Guayas") {
        return [
            { name: "Col. Vicente Rocafuerte", coords: [-2.1884, -79.8973] },
            { name: "Univ. Católica", coords: [-2.1802, -79.9015] },
            { name: "Coliseo Voltaire", coords: [-2.1769, -79.8953] },
            { name: "Edificio Público Norte", coords: [-2.1550, -79.9100] },
            { name: "Albergue Cerro Blanco", coords: [-2.1767, -79.9329] },
            { name: "Hospital Los Ceibos", coords: [-2.1636, -79.9405] }
        ];
    } else {
        // Para otras provincias, generamos 3 albergues aleatorios cercanos (aprox 5km a la redonda)
        return [
            { name: `Hospital Regional de ${provincia}`, coords: [baseLat + 0.04, baseLon + 0.04] },
            { name: `Coliseo de ${provincia}`, coords: [baseLat - 0.04, baseLon - 0.03] },
            { name: `Escuela Segura de ${provincia}`, coords: [baseLat + 0.03, baseLon - 0.05] }
        ];
    }
}

function getClosestSafeZone(lat, lon, safeZones) {
    let closest = null;
    let minDist = Infinity;
    safeZones.forEach(zone => {
        // Distancia euclidiana simple para encontrar el punto más cercano
        const d = Math.pow(zone.coords[0] - lat, 2) + Math.pow(zone.coords[1] - lon, 2);
        if (d < minDist) {
            minDist = d;
            closest = zone;
        }
    });
    return closest;
}

window.trazarRuta = async function(baseLat, baseLon, provincia) {
    // Generamos las zonas seguras específicas para la provincia seleccionada
    const safeZones = getSafeZonesForProvince(baseLat, baseLon, provincia);

    const numPuntos = 3;
    let bounds = L.latLngBounds([]);

    for (let i = 0; i < numPuntos; i++) {
        // Dispersión aleatoria para los puntos vulnerables (aprox 15km a la redonda)
        const lat = baseLat + (Math.random() - 0.5) * 0.15;
        const lon = baseLon + (Math.random() - 0.5) * 0.15;
        
        // Encontrar la zona segura más cercana
        const closestZone = getClosestSafeZone(lat, lon, safeZones);
        const vulName = `Sector Inundado ${i+1}`;
        
        // Petición a OSRM
        const osrmUrl = `https://router.project-osrm.org/route/v1/driving/${lon},${lat};${closestZone.coords[1]},${closestZone.coords[0]}?overview=full&geometries=geojson`;
        
        try {
            const response = await fetch(osrmUrl);
            const data = await response.json();
            if (data.routes && data.routes.length > 0) {
                const route = data.routes[0];
                const coordinates = route.geometry.coordinates.map(c => [c[1], c[0]]);
                
                L.polyline(coordinates, { color: '#58a6ff', weight: 4, dashArray: '10, 10' }).addTo(routeLayer);
                
                // Marcador del Sector Vulnerable con Tooltip Permanente
                L.circleMarker([lat, lon], { color: '#f85149', radius: 5, fillOpacity: 1 }).addTo(routeLayer)
                    .bindTooltip(vulName, { permanent: true, direction: 'bottom', className: 'label-tooltip' });
                    
                // Marcador del Albergue con Tooltip Permanente
                L.circleMarker(closestZone.coords, { color: '#3fb950', radius: 7, fillOpacity: 1, weight: 2, color: '#ffffff' }).addTo(routeLayer)
                    .bindTooltip(closestZone.name, { permanent: true, direction: 'top', className: 'label-tooltip' });
                    
                bounds.extend(coordinates);
            }
        } catch (e) {
            console.error("Error al trazar ruta OSRM:", e);
        }
    }
    
    // Ajustar el zoom
    if (bounds.isValid()) {
        map.fitBounds(bounds, { padding: [50, 50] });
        btnClearRoute.style.display = 'block';
    }
}


async function fetchAndRenderData() {
    btnRefresh.innerText = "Actualizando...";
    
    try {
        const response = await fetch('/api/clima');
        const json = await response.json();
        
        if (json.status === "success") {
            precipitationLayer.clearLayers();
            tideLayer.clearLayers();
            combinedRiskLayer.clearLayers();
            let countRed = 0, countYellow = 0, countBlue = 0, countGreen = 0;
            const isSimStorm = isSimStormActive;
            
            if (!isSimStorm) {
                simulatedProvinces = null;
            } else if (simulatedProvinces === null) {
                // Generar lista de provincias afectadas solo la primera vez que se activa
                simulatedProvinces = ["Guayas"];
                json.data.forEach(p => {
                    if (p.provincia !== "Guayas" && Math.random() < 0.3) {
                        simulatedProvinces.push(p.provincia);
                    }
                });
            }

            const coastalProvinces = ["Guayas", "Esmeraldas", "Manabi", "Santa Elena", "El Oro"];
            json.data.forEach(item => {
                // Sobreescribir datos si el modo simulador está activado
                if (isSimStorm && simulatedProvinces.includes(item.provincia)) {
                    item.precipitacion_mm = (125.8 + Math.random() * 20).toFixed(1); 
                    item.nivel_alerta = "CRÍTICO - Riesgo de Inundación";
                    
                    if (coastalProvinces.includes(item.provincia)) {
                        item.nivel_marea_m = 4.5;
                        item.estado_marea = "Pleamar (Alta)";
                        item.riesgo_combinado = "ALERTA ROJA (Inundación Inminente por Represamiento)";
                    } else {
                        item.riesgo_combinado = "CRÍTICO - Lluvia Extrema";
                    }
                }

                const risk = item.riesgo_combinado || "";
                if (risk.includes("ALERTA ROJA") || risk.includes("CRÍTICO")) countRed++;
                else if (risk.includes("Moderada")) countYellow++;
                else if (risk.includes("Ligera")) countBlue++;
                else countGreen++;

                const isRedAlert = risk.includes("ALERTA ROJA") || risk.includes("CRÍTICO");
                
                // 1. Capa de Lluvia
                if (item.precipitacion_mm !== undefined) {
                    const rColor = getRainColor(item.nivel_alerta);
                    const rCircle = L.circle([item.latitud, item.longitud], {
                        color: rColor, fillColor: rColor, fillOpacity: 0.3, radius: item.nivel_alerta.includes("CRÍTICO") ? 20000 : 12000, weight: 1
                    });
                    rCircle.bindPopup(`<b>${item.provincia}</b><br>🌧️ Lluvia: ${item.precipitacion_mm} mm<br>⚠️ ${item.nivel_alerta}`);
                    precipitationLayer.addLayer(rCircle);
                }

                // 2. Capa de Marea
                if (item.nivel_marea_m > 0) {
                    const tColor = getTideColor(item.nivel_marea_m);
                    // Dibuja un círculo más pequeño adentro
                    const tCircle = L.circle([item.latitud, item.longitud], {
                        color: tColor, fillColor: tColor, fillOpacity: 0.6, radius: 8000, weight: 2
                    });
                    tCircle.bindPopup(`<b>${item.provincia}</b><br>🌊 Marea: ${item.nivel_marea_m.toFixed(2)} m<br>Estado: ${item.estado_marea}`);
                    tideLayer.addLayer(tCircle);
                }

                // 3. Capa de Riesgo Combinado
                if (item.riesgo_combinado) {
                    const cColor = getCombinedRiskColor(item.riesgo_combinado);
                    const cCircle = L.circle([item.latitud, item.longitud], {
                        color: cColor, fillColor: cColor, fillOpacity: isRedAlert ? 0.7 : 0.4, radius: isRedAlert ? 25000 : 15000, weight: 3
                    });
                    
                    let popupContent = `
                        <div class="popup-title">${item.provincia}</div>
                        <p class="popup-data">🌧️ Lluvia: <b>${item.precipitacion_mm} mm</b></p>
                        <p class="popup-data">🌊 Marea: <b>${item.nivel_marea_m.toFixed(2)} m (${item.estado_marea})</b></p>
                        <p class="popup-data">🚨 Riesgo: <span style="color:${cColor}; font-weight:bold">${item.riesgo_combinado}</span></p>
                    `;

                    // Botón de evacuación solo para riesgo alto (o simulable para la demo)
                    if (isRedAlert || item.riesgo_combinado.includes("CRÍTICO")) {
                        popupContent += `<br><button class="glass-btn" onclick="trazarRuta(${item.latitud}, ${item.longitud}, '${item.provincia}')" style="width:100%; padding: 5px; font-size:12px; margin-top:5px;">Generar Ruta de Evacuación</button>`;
                    }

                    cCircle.bindPopup(popupContent);
                    combinedRiskLayer.addLayer(cCircle);
                }
            });
            
            // --- Lógica del Live Feed (Reportes en Vivo) ---
            const feed = document.getElementById('live-feed');
            
            // Elegir aleatoriamente de 2 a 3 provincias para reportar en este ciclo
            const shuffled = [...json.data].sort(() => 0.5 - Math.random());
            let reports = shuffled.slice(0, 2 + Math.floor(Math.random() * 2));
            
            // Si está simulado, forzamos el reporte de Guayas (si no está ya)
            if (isSimStormActive) {
                const guayasItem = json.data.find(p => p.provincia === "Guayas");
                if (guayasItem && !reports.some(r => r.provincia === "Guayas")) {
                    reports.unshift(guayasItem);
                }
            }

            reports.forEach(item => {
                const risk = item.riesgo_combinado || "";
                const isRed = risk.includes("ALERTA") || risk.includes("CRÍTICO");
                const isYellow = risk.includes("Moderada");
                
                let cssClass = "";
                if (isRed) cssClass = "roja";
                else if (isYellow) cssClass = "amarilla";

                // Viento simulado para enriquecer la interfaz (10-60 km/h)
                const wind = Math.floor(10 + Math.random() * 50); 
                let windIcon = wind > 40 ? "🌪️" : "💨";

                const msg = document.createElement('div');
                msg.className = `feed-msg ${cssClass}`;
                msg.innerHTML = `
                    <span class="feed-time">[${new Date().toLocaleTimeString()}]</span>
                    <b>${item.provincia}</b><br>
                    ${windIcon} Viento: ${wind} km/h | 🌧️ Lluvia: ${item.precipitacion_mm} mm.<br>
                    ${risk ? `🚨 <i>${risk}</i>` : 'Estado Normal.'}
                `;
                
                feed.prepend(msg); // Insertar arriba (efecto chat)
            });
            
            // Mantener solo los últimos 20 mensajes para evitar saturación de RAM
            while(feed.children.length > 20) {
                feed.removeChild(feed.lastChild);
            }
            // -----------------------------------------------

            txtTotalRed.innerText = countRed;
            txtTotalYellow.innerText = countYellow;
            txtTotalBlue.innerText = countBlue;
            txtTotalGreen.innerText = countGreen;
            const now = new Date();
            txtLastUpdate.innerText = `Última actualización: ${now.toLocaleTimeString()}`;
            
        } else {
            console.error("Error de la API:", json.message);
        }
    } catch (error) {
        console.error("Fallo de red:", error);
    } finally {
        btnRefresh.innerText = "Actualizar Datos";
    }
}

function updateLayersVisibility() {
    if (map.getZoom() >= 10) return; // Si estamos en nivel de calle, no mostramos círculos

    const isRain = togglePrecip.checked;
    const isTide = toggleTide.checked;

    // Limpiamos todas las capas del mapa temporalmente
    map.removeLayer(precipitationLayer);
    map.removeLayer(tideLayer);
    map.removeLayer(combinedRiskLayer);

    // Lógica combinada
    if (isRain && isTide) {
        map.addLayer(combinedRiskLayer); // Muestra solo combinado si ambos están activos
    } else if (isRain) {
        map.addLayer(precipitationLayer);
    } else if (isTide) {
        map.addLayer(tideLayer);
    }
}

// Manejo de toggles
btnRefresh.addEventListener('click', fetchAndRenderData);

togglePrecip.addEventListener('change', updateLayersVisibility);
toggleTide.addEventListener('change', updateLayersVisibility);

btnSimStorm.addEventListener('click', () => {
    isSimStormActive = !isSimStormActive;
    if (isSimStormActive) {
        btnSimStorm.style.background = 'rgba(248, 81, 73, 0.2)';
        btnSimStorm.innerText = 'Desactivar Tormenta';
    } else {
        btnSimStorm.style.background = 'transparent';
        btnSimStorm.innerText = 'Simular Tormenta';
    }
    fetchAndRenderData();
});

btnClearRoute.addEventListener('click', () => {
    routeLayer.clearLayers();
    map.setView([-2.1962, -79.8862], 7); // Resetear zoom al panorama nacional
    btnClearRoute.style.display = 'none'; // Ocultar el botón después de limpiar
});

// Ocultar círculos grandes al hacer mucho zoom para poder ver la ruta de evacuación
map.on('zoomend', function() {
    if (map.getZoom() >= 10) {
        // Nivel de calle: ocultamos los círculos para ver la ruta
        map.removeLayer(precipitationLayer);
        map.removeLayer(tideLayer);
        map.removeLayer(combinedRiskLayer);
    } else {
        // Nivel provincial: volvemos a evaluar qué capas mostrar según los toggles
        updateLayersVisibility();
    }
});

// Inicial
fetchAndRenderData();
setInterval(fetchAndRenderData, 10000);
