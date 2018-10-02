// Creating our initial map object
// We set the longitude, latitude, and the starting zoom level
// This gets inserted into the div with an id of 'map'
var map = L.map("map", {
    center: [40.75826644897461, -73.93768310546875],
    zoom: 13
});

// Adding a tile layer (the background map image) to our map
// We use the addTo method to add objects to our map
L.tileLayer(
    "https://api.mapbox.com/styles/v1/mapbox/outdoors-v10/tiles/256/{z}/{x}/{y}?" +
    "access_token=pk.eyJ1IjoiZW1jbWlsbGluIiwiYSI6ImNqbGhhMGRlMTAwbHMzcG11NWlwbnNtaWMifQ.lvcx8SdYsDmCDcv24bK5TA"
).addTo(map);

// adding circle 
// L.circle([lat,lon], {
//     color: 'red',
//     fillColor: '#f03',
//     fillOpacity: 0.5,
//     radius: distance
// }).addTo(map)

// Adding Markers and lines
// defining pickup/ dropoff marker styles 
var pickupIcon = new L.Icon({
    iconUrl: 'https://cdn.rawgit.com/pointhi/leaflet-color-markers/master/img/marker-icon-green.png',
    shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
    iconSize: [25, 41],
    iconAnchor: [12, 41],
    popupAnchor: [1, -34],
    shadowSize: [41, 41]
})


var dropoffIcon = new L.Icon({
    iconUrl: 'https://cdn.rawgit.com/pointhi/leaflet-color-markers/master/img/marker-icon-red.png',
    shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
    iconSize: [25, 41],
    iconAnchor: [12, 41],
    popupAnchor: [1, -34],
    shadowSize: [41, 41]
});




// Read into d3
function buildMap () {
    //select values from html 
    lat = d3.select('#lat').property('value')
    lon = d3.select('#lon').property('value')
    distance = d3.select('#distance').property('value')
    maxResults = d3.select('#maxResults').property('value')
    metric = d3.select('#metric').node().selectedOptions[0].value
    tripDistance = d3.select('#tripDistance').property('value')
    tripDistanceFilter = d3.select('#tripDistanceFilter').node().selectedOptions[0].value
    paymentType = d3.select('#paymentType').node().selectedOptions[0].value
    start = d3.select('#start').property('value')
    end = d3.select('#end').property('value')

    // log to check values, url 
    console.log(lat, lon, distance, metric, tripDistance, tripDistanceFilter, paymentType, start, end)
    console.log(`/data?lat=${lat}&lon=${lon}&distance=${distance}&metric=${metric}&maxResults=${maxResults}&tripDistance=${tripDistance}&tripDistanceFilter=${tripDistanceFilter}&paymentType=${paymentType}&start=${start}&end=${end}`)

    // fetch data from /data flask route, passing url parameters
    d3.json(`/data?lat=${lat}&lon=${lon}&distance=${distance}&metric=${metric}&maxResults=${maxResults}&tripDistance=${tripDistance}&tripDistanceFilter=${tripDistanceFilter}&paymentType=${paymentType}&start=${start}&end=${end}`)
        .then(function (data) {
        console.log(data)


        data.forEach(element => {

            // pickup marker
            pickupMarker = L.marker([element.pickup_latitude, element.pickup_longitude], {
                icon: pickupIcon 
            })
            
            pickupMarker.bindPopup(`Latitude: ${element.pickup_latitude} <br> Longitude: ${element.pickup_longitude} <br> Trip Distance: ${element.trip_distance} <br> Payment Type: ${element.payment_type} <br> Pickup Time: ${element.pickup_dt} `);
            pickupMarker.on('mouseover', function (e) {
                this.openPopup();
            });
            pickupMarker.on('mouseout', function (e) {
                this.closePopup();
            });

            pickupMarker.addTo(map)

            // dropoff marker
            dropoffMarker = L.marker([element.dropoff_latitude, element.dropoff_longitude], {
                icon: dropoffIcon 
            })
            
            dropoffMarker.bindPopup(`Latitude: ${element.dropoff_latitude} <br> Longitude: ${element.dropoff_longitude} <br> Trip Distance: ${element.trip_distance} <br> Payment Type: ${element.payment_type} <br> Dropoff Time: ${element.dropoff_dt} `);
            dropoffMarker.on('mouseover', function (e) {
                this.openPopup();
            });
            dropoffMarker.on('mouseout', function (e) {
                this.closePopup();
            });

            dropoffMarker.addTo(map)

            // Connect the dots
            L.polyline([
                [element.pickup_latitude, element.pickup_longitude],
                [element.dropoff_latitude, element.dropoff_longitude]
            ], {color: 'black', weight: 3, opacity: .75, smoothFactor: 1}).addTo(map)

        });
    })
}

